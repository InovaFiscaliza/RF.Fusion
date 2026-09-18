"""Fetch Microsoft Graph photos without exposing the server access token."""

from __future__ import annotations

import os
from hashlib import sha256
from http import HTTPStatus
from pathlib import Path
import re
from tempfile import NamedTemporaryFile
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import HTTPRedirectHandler, Request, build_opener

from . import config as k


class MicrosoftPhotoError(RuntimeError):
    """Report a photo transport, configuration, or storage failure."""


class MicrosoftNotConfigured(MicrosoftPhotoError):
    """Indicate that no server token is available for Microsoft Graph."""


class MicrosoftPhotoNotFound(MicrosoftPhotoError):
    """Indicate that Graph returned no photo for the supplied identity."""


class _RejectRedirects(HTTPRedirectHandler):
    """Keep authorization credentials on the configured Microsoft Graph endpoint."""

    def redirect_request(self, req: Request, fp: object, code: int, msg: str,
                         headers: object, newurl: str) -> Request:
        """Reject redirects before urllib can forward the Authorization header.

        Args:
            req: Original HTTP request. Type: Request.
            fp: Response stream. Type: object.
            code: Redirect status. Type: int.
            msg: Status description. Type: str.
            headers: Response headers. Type: object.
            newurl: Redirect destination. Type: str.

        Returns:
            Request: Never returned because redirects are rejected.

        Raises:
            MicrosoftPhotoError: For every redirect.
        """
        raise MicrosoftPhotoError("O Graph redirecionou a consulta da foto.")


def get_profile_image_url(email: str) -> str:
    """Download a user's photo and return its versioned local public URL.

    Args:
        email: Entra login email (userPrincipalName). Type: str. Surrounding
            whitespace is removed and the address is normalized to lowercase.
            A mail alias that differs from the UPN requires prior resolution.

    Returns:
        Local image URL without credentials. Type: str. Identical image bytes
        yield the same URL; changed bytes yield a new URL. No database writes.

    Raises:
        ValueError: If the supplied email is invalid.
        MicrosoftNotConfigured: If no access token is configured.
        MicrosoftPhotoNotFound: If Graph responds with 404.
        MicrosoftPhotoError: For HTTP, format, size, network, or storage failures.
    """
    email = str(email or "").strip().casefold()
    if not re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", email):
        raise ValueError("Informe o e-mail de login do usuário no Entra.")
    token = _access_token()
    request = Request(
        f"{k.GRAPH_BASE_URL}/users/{quote(email, safe='')}/photo/$value",
        headers={"Authorization": f"Bearer {token}", "Accept": "image/jpeg, image/png"},
    )
    try:
        with build_opener(_RejectRedirects()).open(request, timeout=k.HTTP_TIMEOUT_SECONDS) as response:
            if response.status != HTTPStatus.OK:
                raise MicrosoftPhotoError("Resposta inesperada ao consultar a foto.")
            image_type = response.headers.get_content_type()
            content = response.read(k.MAX_IMAGE_BYTES + 1)
    except HTTPError as error:
        status = error.code
        error.close()
        if status == HTTPStatus.NOT_FOUND:
            raise MicrosoftPhotoNotFound("Foto não encontrada no Microsoft Graph.") from None
        raise MicrosoftPhotoError(f"Falha ao consultar a foto no Graph (HTTP {status}).") from None
    except (URLError, OSError, ValueError):
        raise MicrosoftPhotoError("Não foi possível consultar a foto no Microsoft Graph.") from None

    if not content or len(content) > k.MAX_IMAGE_BYTES:
        raise MicrosoftPhotoError("A foto está vazia ou excede o limite permitido.")
    if image_type not in k.IMAGE_FORMATS:
        raise MicrosoftPhotoError("Formato de foto não suportado pelo WebFusion.")
    extension, signature = k.IMAGE_FORMATS[image_type]
    if not content.startswith(signature):
        raise MicrosoftPhotoError("O conteúdo recebido não corresponde ao formato da foto.")

    filename = f"{sha256(email.encode()).hexdigest()}-{sha256(content).hexdigest()}{extension}"
    target = k.PROFILE_IMAGE_DIRECTORY / filename
    temporary_path = None
    try:
        k.PROFILE_IMAGE_DIRECTORY.mkdir(parents=True, exist_ok=True)
        if not target.exists():
            with NamedTemporaryFile(dir=k.PROFILE_IMAGE_DIRECTORY, delete=False) as temporary:
                temporary_path = Path(temporary.name)
                temporary.write(content)
            # Nginx serves these files under its own user.
            temporary_path.chmod(0o644)
            temporary_path.replace(target)
    except OSError:
        raise MicrosoftPhotoError("Não foi possível armazenar a foto do usuário.") from None
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)
    return f"{k.PROFILE_IMAGE_URL_PREFIX}/{filename}"


def _access_token() -> str:
    """Load the Graph bearer token from the environment or local secret file.

    Args:
        None.

    Returns:
        Nonempty access token. Type: str. Environment overrides the local file.

    Raises:
        MicrosoftNotConfigured: If neither source provides a token.
        MicrosoftPhotoError: If the file is unreadable or the token is invalid.
    """
    token = os.getenv(k.TOKEN_ENV_NAME, "").strip()
    if not token:
        try:
            lines = k.TOKEN_FILE.read_text(encoding="utf-8").splitlines()
        except FileNotFoundError:
            lines = []
        except OSError:
            raise MicrosoftPhotoError("Não foi possível ler a configuração do Graph.") from None
        for line in lines:
            name, separator, value = line.partition("=")
            if separator and name.strip() == k.TOKEN_ENV_NAME:
                token = value.strip()
    if not token:
        raise MicrosoftNotConfigured("Token do Microsoft Graph ainda não configurado.")
    if any(character.isspace() for character in token):
        raise MicrosoftPhotoError("Token do Microsoft Graph inválido.")
    return token
