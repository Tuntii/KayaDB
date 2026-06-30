# kaya-go

Go client for [KayaDB](https://github.com/Tuntii/KayaDB) — implements the TCP wire protocol from `docs/clients/client-wire-protocol.md`.

## Install

```bash
go get github.com/Tuntii/KayaDB/clients/kaya-go
```

## Usage

```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	kaya "github.com/Tuntii/KayaDB/clients/kaya-go"
)

func main() {
	client, err := kaya.Connect("127.0.0.1:7379")
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	client.SetTimeout(3 * time.Second)
	client.SetClientToken("my-client-token") // optional

	ctx := context.Background()

	if err := client.Put(ctx, []byte("user:1"), []byte("ada")); err != nil {
		log.Fatal(err)
	}

	val, err := client.Get(ctx, []byte("user:1"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("GET:", string(val))

	role, err := client.Health(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Node role:", role)

	stats, err := client.Stats(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Stats:", stats)
}
```

## Features

- **Operations:** `Put`, `Get`, `Delete`, `Scan`, `Health`, `Stats`
- **Leader redirect:** follows `STATUS_NOT_LEADER` (10) hints (up to 3 redirects)
- **Client token:** optional `CLIENT\x00` auth prefix via `SetClientToken`
- **Wire format:** little-endian u32 frames, opcodes 1–6 (matches `kaya-net`)

## Testing

```bash
cd clients/kaya-go
go test ./...
```

## Protocol reference

- [client-wire-protocol.md](../../docs/clients/client-wire-protocol.md)
- [go-client.md](../../docs/clients/go-client.md)