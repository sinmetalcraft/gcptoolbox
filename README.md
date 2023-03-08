# gcptoolbox
Command Line Tool for Google Cloud Platform

## Install

```
go install github.com/sinmetalcraft/gcptoolbox@latest
```

## Develop

```
go generate ./...
```

## Example

### monitoring metrics export

```
gcptoolbox monitoring export {PROJECT_ID} storage-total-bytes
```

```
gcptoolbox monitoring export {PROJECT_ID} storage-receive-bytes
```