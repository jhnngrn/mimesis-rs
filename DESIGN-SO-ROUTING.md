# SO-instans-baserad routing — designdokument

## Sammanfattning

Byt från namespace-baserad routing (`"trustline.payment"`) till SO-instans-baserad routing (so_id + method). Varje SO-instans har ett bilateralt delat ID. Meddelanden skickas till en specifik SO-instans, inte till ett namespace.

## Begrepp

- **SO-instans**: en sync-primitiv per peer-relation, identifierad av ett bilateralt ID
- **SO-typ**: t.ex. "trustline", "triad", "novation" — registrerad med SoConfig
- **so_id**: bilateralt delat ID, = countern vid connect-tillfället
- **Root SO**: den första SO:n på en link (t.ex. trustline), skapas vid link.connect
- **Child SO**: SO skapad under en parent (t.ex. novation under triad)

## SO-instans-tabell (per link)

```rust
struct SoInstance {
    so_type: String,            // "trustline", "novation", etc.
    parent_id: Option<u64>,     // None = root
    status: ConnectionStatus,
    children: ChildTracker,
    handler: Box<dyn SoHandler>,
}

struct LinkState {
    instances: HashMap<u64, SoInstance>,
}
```

Root-SO:n skapas vid link.connect. Children skapas via connect_child.

## SoHandler trait (ersätter Module)

```rust
trait SoHandler: Send + Sync {
    fn on_connect(&mut self, peer: &str, payload: Option<Value>);
    fn on_close(&mut self, peer: &str, payload: Option<Value>);
    fn handle_request(
        &mut self, peer: &str, method: &str, 
        payload: Option<Value>, ts: i64
    ) -> Result<HandleResult, Error>;
    fn handle_response(
        &mut self, peer: &str, method: &str, 
        payload: Option<Value>, ok: bool, ts: i64
    );
    fn force_remove(&mut self, peer: &str);
}
```

Varje SO-typ implementerar SoHandler. Ingen "Module" som wrapper.

## Wire-format

```rust
struct TxRequest {
    from: String,
    to: String,
    counter: u64,
    so_id: u64,         // vilken SO-instans (0 = lifecycle/root)
    method: String,     // bara metoden, inte "ns.method"
    payload: Option<Value>,
    timestamp: i64,
    resp: Option<TxResponse>,
}
```

## Dispatch

```rust
fn dispatch_request(&mut self, peer: &str, so_id: u64, method: &str, payload, ts) {
    let instance = link.instances.get_mut(so_id);
    instance.handler.handle_request(peer, method, payload, ts)
}
```

Ingen namespace-parsning, ingen split_method. Direkt lookup via so_id.

## Connect — lifecycle

### Link connect (skapar root SO)
1. Alice: `node.connect(peer, "trustline")` → sätter intent_connect
2. next_tx skickar connect-request (so_id=0, method="connect", payload={type:"trustline"})
3. Bob tar emot → frameworket skapar SoInstance med so_type="trustline", tilldelar so_id
4. on_connect anropas på den nya instansen
5. Response tillbaka → Alice skapar sin instans, on_connect anropas

### Child connect (skapar child SO)
1. Alice's triad tar emot "commit" → i handlern anropar connect_child("novation", peer, payload)
2. Frameworket skickar connect-request (so_id=<counter>, method="connect", payload={type:"novation", ...})
3. Bob tar emot → frameworket ser att "novation" är child under triad
4. Skapar SoInstance med so_type="novation", parent_id=triads so_id
5. on_connect(peer, payload) anropas — detta ÄR factory/konstruktorn
6. Response tillbaka → Alice's SO markeras connected

### So_id-tilldelning
- so_id = link-countern vid connect-transaktionen
- Båda sidor ser samma counter → samma so_id
- Unikt per link

## Close — lifecycle

SO:n kan initiera sin egen close:
1. Novation tar emot "finalize" → bestämmer att den är klar
2. Anropar self.close(payload) 
3. Frameworket skickar close-request (so_id=7, method="close", payload)
4. Peer tar emot → on_close anropas → SO:n rensar upp
5. Frameworket tar bort instansen ur tabellen
6. Response tillbaka → on_close på initierande sidan → instans borttagen

## connect_child — hur SO:n får tillgång

SO-handleren behöver kunna anropa connect_child och close. 
Via en context/handle som skickas in:

```rust
trait SoHandler: Send + Sync {
    fn handle_request(
        &mut self, 
        ctx: &mut SoContext,  // tillgång till connect_child, close, etc.
        peer: &str, 
        method: &str, 
        payload: Option<Value>, 
        ts: i64
    ) -> Result<HandleResult, Error>;
    // ...
}

struct SoContext {
    so_id: u64,
    registry: RegistryRef,
    // metoder: connect_child, close, enqueue, etc.
}
```

## Registry-ändringar

Registry fungerar som idag — queues per SO med queue_id. Men:
- `module`-fältet i SyncQueue ersätts av/kompletteras med `so_id`
- collect_outbox samlar per link+peer, inte per module-namn

## nextTx-ändringar

Samlar från alla SO-instansers queues på en link, väljer äldsta.
Ingen modul-namn-filtrering — bara so_id-baserat.

## Vad som försvinner

- split_method / namespace-routing
- Module trait (ersätts av SoHandler)
- LinkState.modules HashMap<String, ConnectionState>
- dispatch_module_request / dispatch_module_response
- "module.connect" / "module.close" som wire-metoder
- modulnamn i method-strängen

## Vad som står kvar

- Link: counter, turn_bit, validate, accept — orörd
- SyncObject: queue-wrapper — anpassas till so_id
- ChildTracker: ref-räkning — orörd
- Wire-primitiver: TxResponse, OutboxEntry — orörda
- SoConfig: connect_mode, children — orörd
