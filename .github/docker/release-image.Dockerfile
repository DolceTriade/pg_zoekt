ARG BASE_IMAGE=postgres:18-trixie
FROM ${BASE_IMAGE}

COPY dist/package/usr/lib/postgresql/18/lib/ /usr/lib/postgresql/18/lib/
COPY dist/package/usr/share/postgresql/18/extension/ /usr/share/postgresql/18/extension/
