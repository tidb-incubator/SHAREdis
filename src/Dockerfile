FROM swr.ap-southeast-3.myhuaweicloud.com/shareit-common/go-build:v1.14.2 AS build-bin-file

COPY . $GOPATH/src/sharedis/
WORKDIR $GOPATH/src/sharedis
RUN make build && \
    chmod a+x run.sh && \
    mkdir target/ && \
    cp bin/* target/ && \
    cp config/*.toml target/ && \
    cp run.sh target/

FROM swr.ap-southeast-3.myhuaweicloud.com/shareit-common/bin-base:shareit-v0.0.1
COPY --from=build-bin-file /data/code/src/sharedis/target/* /data/sharedis/
WORKDIR /data/sharedis
RUN sudo chown -R ${AppUser}:${AppGroup} /data && \
    mkdir bin/ config/ && \
    mv sharedis bin/ && \
    mv config*.toml config/

CMD ["/bin/bash","-c","/data/sharedis/run.sh"]
