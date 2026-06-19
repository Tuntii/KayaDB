# KayaDB Kullanım Senaryoları Kılavuzu

Bu kılavuz, KayaDB'yi farklı senaryolarda nasıl kullanacağınızı adım adım gösterir. Hem yeni başlayanlar hem de ileri seviye kullanıcılar (operatör, geliştirici, test mühendisi) için pratik örnekler içerir.

**Önkoşullar:** Rust 1.85+, `cargo`. Tüm örnekler hem Unix hem Windows PowerShell için geçerlidir.

KayaDB iki temel modda çalışır:

- **Gömülü (Embedded) mod**: `kayactl --data` ile doğrudan veri dizinine erişim (sunucu gerekmez).
- **Sunucu / Küme modu**: `kayadb-server` çalıştırıp `kayactl --server` veya `kaya-client` ile TCP üzerinden bağlanmak.

---

## İçindekiler

1. [Hızlı Yerel Test (Sunucusuz)](#1-hızlı-yerel-test-sunucusuz)
2. [Tek Düğümlü Sunucu Çalıştırma](#2-tek-düğümlü-sunucu-çalıştırma)
3. [Üç Düğümlü Yerel Küme Kurma](#3-üç-düğümlü-yerel-küme-kurma)
4. [Küme Üyeliği Yönetimi (Add/Remove Node)](#4-küme-üyeliği-yönetimi-addremove-node)
5. [Rust Client ile Programatik Erişim](#5-rust-client-ile-programatik-erişim)
6. [Rust ile Gömülü Motor Kullanımı](#6-rust-ile-gömülü-motor-kullanımı)
7. [Çökme Sonrası Kurtarma ve Doğrulama](#7-çökme-sonrası-kurtarma-ve-doğrulama)
8. [Depolama Dosyalarını İnceleme](#8-depolama-dosyalarını-inceleme)
9. [İstatistik, Gözlemlenebilirlik ve eBPF](#9-istatistik-gözlemlenebilirlik-ve-ebpf)
10. [Otomasyon ve Cluster Scriptleri](#10-otomasyon-ve-cluster-scriptleri)
11. [İleri Seviye: Benchmark ve Simülasyon](#11-ileri-seviye-benchmark-ve-simülasyon)
12. [Yaygın Hatalar ve Çözümleri](#12-yaygın-hatalar-ve-çözümleri)

---

## 1. Hızlı Yerel Test (Sunucusuz)

En basit başlangıç. Herhangi bir sunucu başlatmadan veri yazıp okuyabilirsiniz.

```bash
# Veri dizini oluştur
mkdir -p /tmp/kaya-demo
DATA=/tmp/kaya-demo

# Yaz
cargo run -p kayactl -- --data $DATA put hello world

# Oku
cargo run -p kayactl -- --data $DATA get hello

# Sil
cargo run -p kayactl -- --data $DATA delete hello

# Önek tarama
cargo run -p kayactl -- --data $DATA scan user:

# İstatistik
cargo run -p kayactl -- --data $DATA stats
```

**Windows PowerShell:**
```powershell
$DATA = "$env:TEMP\kaya-demo"
New-Item -ItemType Directory -Force -Path $DATA | Out-Null

cargo run -p kayactl -- --data $DATA put hello world
cargo run -p kayactl -- --data $DATA get hello
cargo run -p kayactl -- --data $DATA stats
```

**Dayanıklılık modu:**
```bash
# Strict (varsayılan): her yazmada fsync
cargo run -p kayactl -- --data $DATA --durability strict put key value

# Relaxed: daha hızlı, daha az garanti
cargo run -p kayactl -- --data $DATA --durability relaxed put key value
```

---

## 2. Tek Düğümlü Sunucu Çalıştırma

Uygulamanız için gerçek bir TCP sunucusu istiyorsanız:

```bash
# Terminal 1 - Sunucuyu başlat
cargo run -p kaya-server --bin kayadb-server \
  -- --data /tmp/kaya-node1 \
     --raft-addr 127.0.0.1:7481 \
     --client-addr 127.0.0.1:7379
```

```bash
# Terminal 2 - Client ile konuş
cargo run -p kayactl -- --server 127.0.0.1:7379 put user:1 ada
cargo run -p kayactl -- --server 127.0.0.1:7379 get user:1
cargo run -p kayactl -- --server 127.0.0.1:7379 scan user:
cargo run -p kayactl -- --server 127.0.0.1:7379 status
```

Sunucu durduktan sonra aynı veri diziniyle yeniden başlatıldığında otomatik olarak kurtarma yapar.

---

## 3. Üç Düğümlü Yerel Küme Kurma

Raft tabanlı dağıtık çalışma için klasik senaryo.

**Manuel başlatma (3 terminal):**

```bash
# Node 1
cargo run -p kaya-server --bin kayadb-server -- \
  --node-id 1 \
  --raft-addr 127.0.0.1:7481 \
  --client-addr 127.0.0.1:7379 \
  --peer 2=127.0.0.1:7482,127.0.0.1:7380 \
  --peer 3=127.0.0.1:7483,127.0.0.1:7381 \
  --data /tmp/kaya-node1

# Node 2
cargo run -p kaya-server --bin kayadb-server -- \
  --node-id 2 \
  --raft-addr 127.0.0.1:7482 \
  --client-addr 127.0.0.1:7380 \
  --peer 1=127.0.0.1:7481,127.0.0.1:7379 \
  --peer 3=127.0.0.1:7483,127.0.0.1:7381 \
  --data /tmp/kaya-node2

# Node 3
cargo run -p kaya-server --bin kayadb-server -- \
  --node-id 3 \
  --raft-addr 127.0.0.1:7483 \
  --client-addr 127.0.0.1:7381 \
  --peer 1=127.0.0.1:7481,127.0.0.1:7379 \
  --peer 2=127.0.0.1:7482,127.0.0.1:7380 \
  --data /tmp/kaya-node3
```

**Script ile başlatma (önerilen):**

```bash
# Linux/macOS
CLUSTER_DIR=/tmp/kayadb-cluster ./scripts/start-cluster.sh

# Durdurmak için
./scripts/stop-cluster.sh
```

Kümede yazma işlemleri leader üzerinden yapılır. Client (`kayactl` veya `kaya-client`) `NOT_LEADER` yanıtı alırsa otomatik olarak leader'a yönlendirilir.

```bash
# Herhangi bir düğüme yazıp okuyabilirsiniz
kayactl --server 127.0.0.1:7379 put order:42 shipped
kayactl --server 127.0.0.1:7380 get order:42
kayactl --server 127.0.0.1:7381 status --json
```

---

## 4. Küme Üyeliği Yönetimi (Add/Remove Node)

**Günlük operasyonlar için runbook:** `docs/runbooks/add-remove-node.md`

**Yeni düğüm ekleme (join + üyelik değişikliği):**

```bash
# Node 4'ü başlat (join-cluster ile)
cargo run -p kaya-server --bin kayadb-server -- \
  --join-cluster \
  --node-id 4 \
  --raft-addr 127.0.0.1:7484 \
  --client-addr 127.0.0.1:7383 \
  --peer 1=127.0.0.1:7481,127.0.0.1:7379 \
  --peer 2=127.0.0.1:7482,127.0.0.1:7380 \
  --peer 3=127.0.0.1:7483,127.0.0.1:7381 \
  --data /tmp/kaya-node4
```

```bash
# Leader'dan üyeliği ekle (herhangi bir düğüm üzerinden --server verilebilir, otomatik yönlendirilir)
kayactl --server 127.0.0.1:7379 add-node 4 127.0.0.1:7484 127.0.0.1:7383

# Durumu kontrol et
kayactl --server 127.0.0.1:7379 status
```

**Düğüm çıkarma:**

```bash
kayactl --server 127.0.0.1:7379 remove-node 4
```

> Not: Raft kümesi 2'den az votera düşemez. Üye değişiklikleri `status` ile takip edilmelidir.

---

## 5. Rust Client ile Programatik Erişim

Uygulamanızdan bağlanmak için `kaya-client` kullanın.

```rust
use std::net::SocketAddr;
use kaya_client::KayaClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr: SocketAddr = "127.0.0.1:7379".parse()?;
    let mut client = KayaClient::connect(addr).await?;

    // Otomatik leader yönlendirmesi ile yaz
    client.put(b"order:100", b"shipped").await?;

    // Oku
    if let Some(val) = client.get(b"order:100").await? {
        println!("Değer: {}", String::from_utf8_lossy(&val));
    }

    // İstatistik al
    let stats = client.stats().await?;
    println!("Stats: {}", stats);

    // Sil
    client.delete(b"order:100").await?;

    Ok(())
}
```

**Linearizability tracing (test için):**

```rust
let mut client = KayaClient::connect(addr).await?;
client.enable_tracing();

// ... işlemleri yap ...

if let Some(trace) = client.take_trace(42) {
    println!("Trace: {}", trace);
}
let check = client.check_trace();
```

---

## 6. Rust ile Gömülü Motor Kullanımı

KayaDB'yi kendi uygulamanızın içinde (SQLite/RocksDB gibi) kullanmak için:

```rust
use std::sync::Arc;
use kaya_core::{DurabilityMode, EngineConfig};
use kaya_engine::{Engine, ReadOptions, WriteOptions};
use kaya_io::FileDisk;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let data_dir = std::env::temp_dir().join("myapp_kaya");
    let config = EngineConfig {
        data_dir: data_dir.clone(),
        ..EngineConfig::default()
    };
    let disk = Arc::new(FileDisk::new(data_dir));
    let mut engine = Engine::open(config, disk).await?;

    engine
        .put(
            b"config:theme".to_vec(),
            b"dark".to_vec(),
            WriteOptions { durability: Some(DurabilityMode::Strict), idempotency_key: None },
        )
        .await?;

    let theme = engine.get(b"config:theme", ReadOptions::default()).await?;
    println!("Tema: {:?}", theme.map(|v| String::from_utf8_lossy(&v)));

    Ok(())
}
```

Daha fazla detay için: `crates/kaya-engine/examples/embedded.rs`

---

## 7. Çökme Sonrası Kurtarma ve Doğrulama

KayaDB, WAL + manifest sayesinde çökmelerden sonra tutarlı durumu kurtarır.

```bash
# Kurtarmayı simüle et (hiçbir şey yazmaz)
kayactl --data /tmp/kaya-node1 recover --dry-run

# JSON ile otomasyon için
kayactl --data /tmp/kaya-node1 recover --dry-run --json
```

**Ne zaman kullanmalı?**
- Sunucuyu yeniden başlatmadan önce
- Veri dizinini başka bir yere taşımadan önce
- Şüpheli bir çökme sonrası durumu doğrulamak için

Kurtarma raporu `wal_records_replayed`, `orphaned_sstables`, uyarılar vb. bilgileri içerir.

---

## 8. Depolama Dosyalarını İnceleme

Tüm kalıcı formatlar insan tarafından okunabilir şekilde tasarlanmıştır.

```bash
DATA=/tmp/kaya-node1

# WAL segmenti
kayactl inspect wal $DATA/wal-000001.wal
kayactl inspect wal $DATA/wal-000001.wal --json

# SSTable
kayactl inspect sstable $DATA/sst-000001.sst

# Manifest (LSM meta verisi)
kayactl inspect manifest $DATA/MANIFEST
```

**İnceleme çıktısı örnekleri:**
- WAL: offset, LSN, PUT/DEL, CRC durumu, key/value
- SSTable: footer, index, bloom filter, bloklar
- Manifest: FLUSH ve COMPACT olayları

Bu sayede harici araçlara ihtiyaç duymadan veri bütünlüğünü inceleyebilirsiniz.

---

## 9. İstatistik, Gözlemlenebilirlik ve eBPF

```bash
# Temel istatistikler
kayactl --data $DATA stats
kayactl --server 127.0.0.1:7379 status

# JSON (script / dashboard için)
kayactl --json --data $DATA stats
kayactl --json --server 127.0.0.1:7379 status

# Sadece dayanıklılık ve flush/compaction metrikleri
kayactl --data $DATA stats --latency
```

**Linux eBPF ile kernel seviyesinde gözlem (Track A):**

```bash
# Mevcut düğümleri ve PID'leri bul
kayactl ebpf list
kayactl ebpf status

# fsync gecikmelerini izle
kayactl ebpf fsync-latency --pid 12345 --run

# Blok I/O gecikmeleri
kayactl ebpf block-latency --pid 12345

# Sistem çağrısı zaman çizelgesi (yazma + fsync + flush/compaction korelasyonu)
kayactl ebpf syscall-timeline --pid 12345 --run --duration 30s
```

> eBPF komutları sadece Linux'ta çalışır. Diğer platformlarda yardımcı mesaj ve script konumu gösterir.

**Flush'i tetikleyerek metrikleri güncelle:**

```bash
cargo run -p kayactl -- --data $DATA flush
cargo run -p kayactl -- --data $DATA stats --latency
```

---

## 10. Otomasyon ve Cluster Scriptleri

Proje kökünde `scripts/` altında kullanışlı scriptler vardır:

| Script | Amaç |
|--------|------|
| `start-cluster.sh` / `.ps1` | 3 düğümlü kümeyi başlat |
| `stop-cluster.sh` / `.ps1` | Kümeyi durdur |
| `restart-node.sh` | Tek düğümü yeniden başlat |
| `kill-node.sh` | Düğümü öldür (kaos testi için) |
| `partition-node.sh` | Ağ bölünmesi simüle et |
| `heal-partition.sh` | Bölünmeyi onar |
| `bench-report.sh` | Benchmark raporu |

Örnek:
```bash
CLUSTER_DIR=/tmp/test-cluster ./scripts/start-cluster.sh
# ... testler ...
./scripts/stop-cluster.sh
```

Windows için `.ps1` versiyonları mevcuttur.

---

## 11. İleri Seviye: Benchmark ve Simülasyon

**Benchmark çalıştırma:**
```bash
cargo bench -p kaya-bench
```

Sonuçlar `target/criterion/` altında saklanır. Detaylı karşılaştırma için [BENCHMARKS.md](../BENCHMARKS.md) dosyasına bakın.

**Simülasyon testleri (doğruluk odaklı):**
```bash
cargo test -p kaya-sim
cargo test -p kaya-jepsen-test
```

Simülasyon, aynı tohum ile tekrarlanabilir hata enjeksiyonu ve doğrusal tutarlılık denetimi sağlar. Gerçek çökme senaryolarını güvenli şekilde test etmek için idealdir.

**Fuzzing:**
```bash
cargo +nightly fuzz run fuzz_wal_decoder
# vb.
```

---

## 12. Yaygın Hatalar ve Çözümleri

| Hata / Belirti | Olası Sebep | Çözüm |
|----------------|-------------|-------|
| `address already in use` | Port başka süreç tarafından kullanılıyor | `stop-cluster` çalıştırın veya farklı port seçin |
| `not leader` veya `NOT_LEADER` | Komut follower düğüme gitti | `kayactl` ve `kaya-client` otomatik yönlendirir. Manuel deniyorsanız leader'ın client adresini kullanın |
| `NOT_FOUND` | Anahtar yok veya yanlış veri dizini/küme | Doğru sunucuya veya doğru `--data` dizinine bağlandığınızdan emin olun |
| Kurtarma uyarısı | WAL veya manifest'te sorun | `recover --dry-run --json` çalıştırın, ilgili dosyaları `inspect` ile inceleyin |
| Bağlantı reddedildi | Sunucu çalışmıyor | `kayadb-server`'ı başlatın |
| Kamuya açık ağ uyarısı | `--allow-public-bind` olmadan dış IP ile bağlanmaya çalışılıyor | Önce [security.md](security.md) okuyun. Varsayılan olarak localhost önerilir |

---

## Sonraki Adımlar

- [CLI Referansı](cli-reference.md) — Tüm bayraklar, çıktılar ve çıkış kodları
- [Getting Started](getting-started.md) — Temel kurulum ve ilk adımlar
- [Mimari](architecture.md) — İç işleyişi anlamak için
- [Geliştirme Kılavuzu](development.md) — Test yazma, simülasyon çalıştırma
- [Güvenlik](security.md) — Üretim öncesi önemli uyarılar

KayaDB'yi deneyin, kırın, yeniden oynatın ve karşılaştığınız her şeyi tekrarlanabilir bir teste dönüştürün. Bu, projenin temel felsefesidir.

---

**İyi testler!**
