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
gcptoolbox monitoring export storage-total-bytes {PROJECT_ID}
```

```
gcptoolbox monitoring export storage-receive-bytes {PROJECT_ID} 
```