#!/bin/bash

# This script is used to deploy the appCataloga application to the development server, creating hard links from the repository to the test folders

# test if /usr/local/appCataloga exists, if not, create it
if [ ! -d /usr/local/appCataloga ]; then
    mkdir /usr/local/appCataloga
fi

# test if /etc/appCataloga exists, if not, create it
if [ ! -d /etc/appCataloga ]; then
    mkdir /etc/appCataloga
fi

# check if folders ./root/etc/appCataloga and ./root/usr/local/appCataloga exist, if not, exit with error message
if [ ! -d ./root/etc/appCataloga ] || [ ! -d ./root/usr/local/appCataloga ]; then
    echo "Error: Run this script from /src/appCataloga folder, such as to have ./root/etc/appCataloga and ./root/usr/local/appCataloga folders accessible."
    exit 1
fi

# create hard links from ./root/etc/appCataloga to /etc/appCataloga
ln -f ./root/etc/appCataloga/* /etc/appCataloga

# create hard links from ./root/usr/local/appCataloga to /usr/local/appCataloga
ln -f ./root/usr/local/appCataloga/* /usr/local/appCataloga