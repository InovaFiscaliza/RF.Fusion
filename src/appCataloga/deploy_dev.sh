#!/bin/bash
# Deploy appCataloga no container Debian sem systemd/SELinux

MINICONDA_PATH="/opt/conda"
REPO_ROOT_PATH="/RFFusion/src/appCataloga/root"
CONF_PATH="/etc/appCataloga"
APP_PATH="/usr/local/bin/appCataloga"
LOG_FILE="/var/log/appCataloga.log"

repo_conf=$REPO_ROOT_PATH$CONF_PATH
repo_app=$REPO_ROOT_PATH$APP_PATH

# 1. Verifica pastas do repositório
if [ ! -d "$repo_conf" ] || [ ! -d "$repo_app" ]; then
    echo "❌ Erro: configure REPO_ROOT corretamente. Estrutura não encontrada."
    exit 1
fi

# 2. Remove versões antigas
rm -rf "$APP_PATH" "$CONF_PATH"
mkdir -p "$APP_PATH" "$CONF_PATH"

# 3. Cria hard links dos arquivos de configuração
for file in $(find "$repo_conf" -type f); do
    ln -f "$file" "$CONF_PATH"
done
echo "✅ Configurações copiadas para $CONF_PATH"

# 4. Cria hard links dos arquivos da aplicação
for file in $(find "$repo_app" -type f); do
    ln -f "$file" "$APP_PATH"
done
echo "✅ Aplicação copiada para $APP_PATH"

# 5. Remove log antigo
rm -f "$LOG_FILE"

# 6. Cria symlink do Miniconda
ln -sfn "$MINICONDA_PATH" "$APP_PATH/miniconda3"
echo "✅ Link simbólico para Miniconda criado em $APP_PATH/miniconda3"

# 7. Ajusta permissão de execução para todos os .sh
chmod +x "$APP_PATH"/*.sh
echo "✅ Permissões de execução ajustadas"

echo "🚀 Deploy concluído! Agora use os scripts de controle (ex.: tool_start_all.sh)"
