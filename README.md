# CryptoVision backend

FastAPI szolgaltatas a CryptoVision dashboardhoz. A kulso piaci adatokat a backend
gyorsitotarazza, igy a bongeszo nem kozvetlenul terheli az adatszolgaltatokat.

## Helyi inditas

```powershell
python -m venv .venv
.venv\Scripts\pip install -r requirements-dev.txt
.venv\Scripts\uvicorn main:app --reload
```

Az API dokumentacioja: `http://localhost:8000/docs`

## Fo vegpontok

- `GET /health` - uzemkeszsegi ellenorzes
- `GET /api/v1/dashboard?coin=bitcoin&horizon=7` - teljes dashboard adatcsomag
- `GET /api/v1/forecast?coin=bitcoin&horizon=7` - horizont-specifikus jelzes
- `GET /api/v1/forecast/analytics?coin=bitcoin&horizon=7` - visszameres es naplo
- `GET /api/v1/forecast/lab?coin=bitcoin&horizon=7` - igeny szerinti oras modelllabor
- `GET /api/v1/forecast/registry?coin=bitcoin&horizon=7` - champion/challenger es feature-store allapot
- `GET /api/v1/forecast/data-health` - tarhely-, collector- es tanitasi adatsor monitor
- `GET /api/v1/derivatives?coin=bitcoin` - funding, open interest es pozicionalasi kontextus
- `GET /api/v1/markets?limit=200` - legfeljebb 200 eszkozos, rangsorolt piaclista
- `GET /api/v1/news` - friss hirek
- `GET /api/v1/news/sentiment?coin=bitcoin` - eszkozspecifikus hirhangulat es auditadatok
- `POST /api/v1/internal/snapshots/collect` - vedett, utemezett feature-snapshot gyujto

A regi `/market-overview`, `/crypto-data`, `/crypto-news` es
`/crypto-indicators` vegpontok az atallas idejere tovabbra is elerhetok.

Az arfolyamkatalogus es az elorejelzesi kor kulon adatfolyam. A katalogus
legfeljebb 200 rangsorolt eszkozt ad vissza, mig modell- es hirhangulat-elemzes
10 nagy, likvid, nem-stabil eszkozre keszul. Az API minden sornal jelzi az
`analysis_available` allapotot. CoinGecko-kimaradas eseten a katalogus a
Binance USDT spot parjaibol, 24 oras forgalom szerint rangsorolva epul fel; a
spot piacon nem szereplo elemzesi eszkozt Binance USD-M futures ar egesziti ki.
A `ranking_basis` es a soronkenti `price_source` mezobol egyertelmuen latszik,
melyik rangsor es arforras ervenyes.

A hirek alapbol a CoinDesk, a Decrypt es a Cointelegraph RSS-csatornaibol
erkeznek, ezert kulon API-kulcs nelkul is mukodnek. A
`CRYPTOCOMPARE_API_KEY` opcionakent egy tovabbi hirforrast kapcsol be.

Az elorejelzes kiserleti, kalibralt horizont-specialista ensemble v5.1. Az 1
napos tavon a Huber regresszio, a Huber Gradient Boosting es az Extra Trees, 7
napra a Gradient Boosting, az Extra Trees es a Huber, 30 napra pedig a Ridge, a
Gradient Boosting es az Extra Trees versenyez.
Mindharom csak multbeli ar-, trend-, volatilitas-, RSI-, forgalmi es funding
jellemzoket hasznal. A napi hozamok 2-7 napos lagjei kulon temporal feature
formajaban kerulnek be. A technikai v2 modell biztonsagi tartalekkent megmaradt.

A valoszinusegi challenger keszlet horizontonkent elter. Az 1 napos tavon a
HistGradientBoosting egy temporal Extra Trees modellel versenyez, 7 napra a
regularizalt Logistic Regression es a HistGradientBoosting indul, 30 napra a
kisebb fuggetlen mintaszam miatt csak a konzervativ Logistic Regression.

A napi tanitas kilenc eszkoznel elsodlegesen a Binance nyilvanos USDT spot
OHLCV adataibol legfeljebb 2000 napot hasznal. A HYPE idosora a Hyperliquid
hivatalos `candleSnapshot` API-jabol erkezik, Binance USD-M futures
tartalekkal. Beallitott `CRYPTOCOMPARE_API_KEY` eseten a CryptoCompare
masodlagos forras lehet, vegso tartalek a CoinGecko 365 napos idosora.

A korabbi mintak tanulo, modellvalaszto validacios es erintetlen holdout
szakaszra valnak szet. A specialista algoritmusat csak a validacios szakasz
valasztja ki. A gyoztes csak akkor kap sulyt, ha a kulon holdouton is
felulteljesiti a valtozatlan arat feltetelezo alapmodellt, eleg aktiv jelzest
ad, es azok iranytalalati aranya is eleri a kuszobot. Bizonyitott elony nelkul
a v5.1 a korabbi technikai modellnel marad. A `confidence` visszamert jelminosegi
pontszam, nem jovobeli valoszinuseg.

A v5 kulon, pontosan definialt esemenyvaloszinuseget is becsul:

- 1 nap: `P(hozam >= +0.2%)`
- 7 nap: `P(hozam >= +1%)`
- 30 nap: `P(hozam >= +3%)`

A horizont-specifikus jeloltek idorendi train, kalibracios, validacios es
erintetlen holdout szakaszon versenyeznek. A
szakaszhatarok korul a teljes elorejelzesi horizont ki van zarva. A Platt
kalibraciot validacion valasztott konzervativ zsugoritas fogja vissza, majd a
kapu Brier score, log loss, ROC AUC, kalibracios hiba, harom holdout idoblokk es
ket korabbi teljes kapuvizsgalat es standardizalt feature-drift alapjan dont.
Sikertelen kapu eseten az API nem
a modelljelolt szazalekat, hanem a historikus esemenyaranyt publikalja.

Az API 80%-os empirikus arsavot is visszaad. Ez a korabbi holdout hibak
eloszlasabol keszul, es nem garantalt arfolyamtartomany.

Az elo elorejelzeseket a backend 15 perces idosavonkent naploba menti. Explicit
`FORECAST_DATABASE_URL` eseten PostgreSQL-t, ennek hianyaban a
`FORECAST_DB_PATH` helyen levo SQLite adatbazist hasznalja. A visszameres walk-forward
modszert hasznal: minden tortenelmi tesztpont csak a korabban mar elerheto
arfolyamadatokat latja. Az eredmenyt a valtozatlan arat feltetelezo
alapmodellel es a specialista nelkuli v2 technikai modellel is osszehasonlitja.
Az uj naplobejegyzesek az esemenyvaloszinuseget, az alapeselyt, a celhozamot es
a valoszinusegi kapu allapotat is taroljak. A v5.1 a teljes publikus becslest,
az iranyt es a 80%-os intervallumot is a point-in-time snapshothoz koti.

Ugyanebbe az adatbazisba valtoztathatatlan point-in-time feature snapshot is
kerul: piaci allapot, technikai mutatok, Binance USD-M futures kontextus,
VADER + kriptos penzugyi lexikonos hirhangulat es az aktualis modellallapot. A
sentiment egyelore `context_only`, 0%-os elorejelzesi sullyal szerepel. Ez a kesobbi
leakage-mentes ujratanitas alapja. Az open interest, globalis long/short es taker
statisztikak publikus Binance tortenete jelenleg legfeljebb 30 nap, ezert ezek
meg csak gyujtott challenger-adatok; a hosszabb funding-idosor mar a v5
valoszinusegi feature-keszletenek resze.

A horizont lejarata utan az elso uj piaci megfigyeles idempotens
`feature_outcome` cimket keszit. Ez tarolja a tenyleges arat, a realizalt
hozamot, az esemeny bekovetkezeset es a cimke keseset, igy ugyanaz a snapshot
tobbszori collector-futasnal sem irhato felul. A registry es az analytics
`training_readiness` mezoje kulon mutatja a nyers, lezart, kesesben levo es napi
szinten fuggetlen mintakat. A tanitasi kapu csak tartos tarhely es a
horizont-specifikus minimum elerese utan valhat kesz allapotuva.

A webes analytics keres az elso, koltseges visszamerest hatterfeladatkent
inditja, es `202 pending` allapottal kerheto le ujra. A dashboard 60 lezart
pontot es legfeljebb 60 napos refit-suruseget hasznal a gyors auditban; a
parancssori benchmark megtartja a reszletesebb beallitasokat. A riport a
publikus modell es a tartalekban levo challenger meroszamat kulon mutatja.
Az `live_performance` blokk ettol fuggetlenul csak a tenylegesen publikalt es
mar lejart elorejelzeseket meri 7, 30 es 90 napos ablakban. MAE-t, RMSE-t,
semleges alapmodellhez viszonyitott skillt, aktiv iranytalalatot, intervallum-
lefedettseget es Brier-score-t kozol; keves mintanal `collecting` allapotban
marad.

A teljes napi valoszinusegi benchmark megismetelheto:

```powershell
python -m scripts.benchmark_probability
python -m scripts.benchmark_probability --coins bitcoin --horizons 30 --walk-forward-samples 90
```

Az LSTM nem resze az aktualis eles modellnek. A napi, eszkozonkent korlatozott
mintan a kisebb, horizont-specifikus modellek stabilabban kalibralhatok, es a
Render ingyenes peldanyan lenyegesen gyorsabban indulnak. LSTM csak tobbeves
oras adatkeszleten, kulon purged walk-forward benchmark megnyerese utan kerulhet
be; a jelenlegi oras Modelllabor ezt az osszehasonlitast kesziti elo.

## Oras modelllabor

Az 1 es 7 napos tavhoz a backend igeny szerint legfeljebb 6480 darab egyoras
OHLCV gyertyat is elemez. A HYPE eseteben a Hyperliquid API dokumentalt
korlatja miatt legfeljebb 5000 gyertya hasznalhato. Az iranyjelolt Huber, Ridge
es Huber Gradient Boosting kozul
validacion valaszt, majd purged holdouton kapuz. A 80%-os mozgasi sav kulon
kvantilismodellt hasznal, amelynek egy kedvezo holdout mellett harom korabbi
idoablakbol legalabb kettoben is elonyt kell mutatnia.

A labor nem fut le a normal dashboard keresekor. A kulon vegpont inditja, az
eredmenyt a backend ot percig gyorsitotarazza. A teljes, parancssori benchmark a
`scripts/benchmark_intraday.py` es `scripts/benchmark_intraday_risk.py`
eszkozokkel ismetelheto meg.

## Eles naplotarolas

Helyben az alapertelmezett `data/forecasts.sqlite3` fajl elegendo. Elesben az
ajanlott beallitas egy PostgreSQL kapcsolat:

```text
FORECAST_DATABASE_URL=postgresql://user:password@host:5432/database
```

A backend csak ezt az explicit valtozot olvassa; egy mas szolgaltatasbol maradt
`DATABASE_URL` nem aktivalja veletlenul az adatbazist. Ha a valtozo nincs
beallitva, a rendszer automatikusan SQLite-ra ter vissza. A `GET /health`
valasz `storage` mezoje jelzi az aktiv backendet.

A `FORECAST_STORAGE_LIMIT_MB` a szolgaltatoi tarhelykeretet adja meg a
monitorozashoz, a `SNAPSHOT_STALE_AFTER_MINUTES` pedig azt a kesest, amely utan
a collector mar elavult allapotunak szamit. Az alapertelmezett ertekek a Neon
Free 512 MB-os keretehez es a 15 perces utemezeshez illeszkednek:

```text
FORECAST_STORAGE_LIMIT_MB=512
SNAPSHOT_STALE_AFTER_MINUTES=45
```

A `/api/v1/forecast/data-health` titkok nelkul kozli az adatbazis meretet,
kihasznaltsagat, a collector frissesseget, valamint mind a 30 eszkoz/idotav
adatsor snapshot-, cimke- es tanitasi keszultseget.

A Render alap fajlrendszere nem tartos, ezert az ingyenes SQLite uzemmod
deploy vagy peldany-ujrainditas utan elveszitheti a korabbi mintakat.
Alternativa egy csatolt persistent disk es azon beluli utvonal, peldaul
`FORECAST_DB_PATH=/var/data/forecasts.sqlite3`. A lemez nem kapcsolodik be
automatikusan, mert csak tamogatott, fizetos szolgaltatasi csomaghoz adhato.
Reszletek a
[Render persistent disk dokumentaciojaban](https://render.com/docs/disks) vannak.

Az aktualis koltseghatekony konfiguracio Neon Free PostgreSQL-t hasznal az
AWS Oregon regioban, poolozott SSL-kapcsolattal. A backend szolgaltatofuggetlen
PostgreSQL URL-t olvas, ezert a kesobbi fizetos Render vagy Neon csomagra valtas
alkalmazaskod-modositas nelkul elvegezheto.

## Utemezett snapshot gyujtes

A `.github/workflows/collect-snapshots.yml` workflow 15 percenkent egyetlen
erme/idotav part gyujt. A 40 slotos rotacio mind a tiz tamogatott eszkozt es az
1, 7, illetve 30 napos horizontot lefedi; a likvidebb modellek surubb mintat
kapnak. Az endpoint ugyanazt a dashboard- es modellfolyamatot hasznalja,
mint a normal felulet, majd deduplikalva rogzit elorejelzest es point-in-time
feature snapshotot.

Az aktivalashoz ugyanazt az eros, veletlen tokent kell beallitani ket helyen:

- Render environment: `SNAPSHOT_TOKEN`
- GitHub repository secret: `SNAPSHOT_TOKEN`

Token nelkul a gyujto `503`, hibas tokennel `401` valaszt ad. Kezi ellenorzeshez
a workflow a GitHub Actions feluleterol is indithato. Celzott kezi gyujtesnel a
vedett vegpont a `coin` es `horizon` query parametert egyutt fogadja el.
A workflow minden gyujtes utan ellenorzi a PostgreSQL tartossagat, a collector
frissesseget es azt is, hogy legalabb egy snapshot tenylegesen elerheto.
