ARG BASE_IMAGE=postgres:18-trixie
FROM ${BASE_IMAGE}

COPY dist/package/usr/lib/postgresql/18/lib/ /usr/lib/postgresql/18/lib/
COPY dist/package/usr/share/postgresql/18/extension/ /usr/share/postgresql/18/extension/

RUN if grep -Eq "^[# ]*shared_preload_libraries = " /usr/share/postgresql/postgresql.conf.sample; then \
        sed -Ei "s|^[# ]*shared_preload_libraries = .*|shared_preload_libraries = 'pg_zoekt'|" /usr/share/postgresql/postgresql.conf.sample; \
    else \
        echo "shared_preload_libraries = 'pg_zoekt'" >> /usr/share/postgresql/postgresql.conf.sample; \
    fi && \
    grep "^shared_preload_libraries = 'pg_zoekt'$" /usr/share/postgresql/postgresql.conf.sample
