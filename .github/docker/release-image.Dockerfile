ARG BASE_IMAGE=postgres:18-trixie
FROM ${BASE_IMAGE}

COPY dist/package/usr/lib/postgresql/18/lib/ /usr/local/lib/postgresql/
COPY dist/package/usr/share/postgresql/18/extension/ /usr/local/share/postgresql/extension/
