#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
ROOT_DIR="${SCRIPT_DIR}/.."
PACKAGE_DIR="${ROOT_DIR}/package"
MANIFEST_FILE="${PACKAGE_DIR}/manifest.yml"
ENV_FILE="${ROOT_DIR}/project.env"

RABBITMQ_BRANCH="${1#v}"
RABBITMQ_BASE_VERSION="$(echo ${RABBITMQ_BRANCH} | cut -d'.' -f1,2)"

[ -z "$RABBITMQ_BRANCH" ] && { echo "The current RabbitMQ build branch is not set"; exit 1; }

PKG_VERSION="${2#v}"

[ -z "$PKG_VERSION" ] && { echo "The package version is not set"; exit 1; }

echo "Setting up Classic-b-gone build, v${PKG_VERSION} for ${RABBITMQ_BRANCH} (base version ${RABBITMQ_BASE_VERSION})"

if yq --version 2>&1 | grep -qi 'mikefarah'; then
  YQ_ARGS=( "-i" )
else
  YQ_ARGS=( "-i" "-y" )
fi

yq "${YQ_ARGS[@]}" "
    .version = \"${PKG_VERSION}\" |
    .artifact = \"seven_state_classic_b_gone-${PKG_VERSION}.ez\" |
    .rabbitmq.min = \"${RABBITMQ_BASE_VERSION}\" |
    .rabbitmq.max = \"${RABBITMQ_BRANCH}\"
  " "$MANIFEST_FILE"

echo "PROJECT_VERSION=${PKG_VERSION}" >> "${ENV_FILE}"
echo "RABBITMQ_BASE=${RABBITMQ_BRANCH}" >> "${ENV_FILE}"