FROM gcr.io/distroless/static-debian11
COPY ./gcptoolbox /gcptoolbox
ENTRYPOINT ["/gcptoolbox"]