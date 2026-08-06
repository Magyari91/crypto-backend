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
- `GET /api/v1/derivatives?coin=bitcoin` - funding, open interest es pozicionalasi kontextus
- `GET /api/v1/markets` - piaci lista
- `GET /api/v1/news` - friss hirek

A regi `/market-overview`, `/crypto-data`, `/crypto-news` es
`/crypto-indicators` vegpontok az atallas idejere tovabbra is elerhetok.

A hirek alapbol a CoinDesk, a Decrypt es a Cointelegraph RSS-csatornaibol
erkeznek, ezert kulon API-kulcs nelkul is mukodnek. A
`CRYPTOCOMPARE_API_KEY` opcionakent egy tovabbi hirforrast kapcsol be.

Az elorejelzes kiserleti, kalibralt horizont-specialista ensemble v5. Az 1 napos tavhoz
kiugro ertekekre robusztus Huber regresszio, a 7 napos tavhoz nemlinearis Huber
Gradient Boosting, a 30 napos tavhoz regularizalt Ridge regresszio tartozik.
Mindharom csak multbeli ar-, trend-, volatilitas-, RSI-, forgalmi es funding
jellemzoket hasznal. A napi hozamok 2-7 napos lagjei kulon temporal feature
formajaban kerulnek be. A technikai v2 modell biztonsagi tartalekkent megmaradt.

A valoszinusegi challenger keszlet horizontonkent elter. Az 1 napos tavon a
HistGradientBoosting egy temporal Extra Trees modellel versenyez, 7 napra a
regularizalt Logistic Regression es a HistGradientBoosting indul, 30 napra a
kisebb fuggetlen mintaszam miatt csak a konzervativ Logistic Regression.

A napi tanitas elsodlegesen a Binance nyilvanos USDT OHLCV adataibol legfeljebb
2000 napot hasznal. Beallitott `CRYPTOCOMPARE_API_KEY` eseten a CryptoCompare
masodlagos forras lehet, vegso tartalek a CoinGecko 365 napos idosora.

A korabbi mintak tanulo, kalibracios es erintetlen holdout szakaszra valnak
szet. A specialista csak akkor kap sulyt, ha a holdouton felulteljesiti a
valtozatlan arat feltetelezo alapmodellt, eleg aktiv jelzest ad, es azok
iranytalalati aranya is eleri a kuszobot. Bizonyitott elony nelkul a v5 a
korabbi technikai modellnel marad. A `confidence` visszamert jelminosegi
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

Az elo elorejelzeseket a backend 15 perces idosavonkent a
`FORECAST_DB_PATH` helyen levo SQLite naploba menti. A visszameres walk-forward
modszert hasznal: minden tortenelmi tesztpont csak a korabban mar elerheto
arfolyamadatokat latja. Az eredmenyt a valtozatlan arat feltetelezo
alapmodellel es a specialista nelkuli v2 technikai modellel is osszehasonlitja.
Az uj naplobejegyzesek az esemenyvaloszinuseget, az alapeselyt, a celhozamot es
a valoszinusegi kapu allapotat is taroljak.

Ugyanebbe az adatbazisba valtoztathatatlan point-in-time feature snapshot is
kerul: piaci allapot, technikai mutatok, Binance USD-M futures kontextus,
headline-lexikonos hirhangulat es az aktualis modellallapot. Ez a kesobbi
leakage-mentes ujratanitas alapja. Az open interest, globalis long/short es taker
statisztikak publikus Binance tortenete jelenleg legfeljebb 30 nap, ezert ezek
meg csak gyujtott challenger-adatok; a hosszabb funding-idosor mar a v5
valoszinusegi feature-keszletenek resze.

A webes analytics keres az elso, koltseges visszamerest hatterfeladatkent
inditja, es `202 pending` allapottal kerheto le ujra. A dashboard 60 lezart
pontot es legfeljebb 60 napos refit-suruseget hasznal a gyors auditban; a
parancssori benchmark megtartja a reszletesebb beallitasokat. A riport a
publikus modell es a tartalekban levo challenger meroszamat kulon mutatja.

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

Az 1 es 7 napos tavhoz a backend igeny szerint 6480 darab Binance 1 oras OHLCV
gyertyat is elemez. Az iranyjelolt Huber, Ridge es Huber Gradient Boosting kozul
validacion valaszt, majd purged holdouton kapuz. A 80%-os mozgasi sav kulon
kvantilismodellt hasznal, amelynek egy kedvezo holdout mellett harom korabbi
idoablakbol legalabb kettoben is elonyt kell mutatnia.

A labor nem fut le a normal dashboard keresekor. A kulon vegpont inditja, az
eredmenyt a backend ot percig gyorsitotarazza. A teljes, parancssori benchmark a
`scripts/benchmark_intraday.py` es `scripts/benchmark_intraday_risk.py`
eszkozokkel ismetelheto meg.

## Eles naplotarolas

Helyben az alapertelmezett `data/forecasts.sqlite3` fajl elegendo. A Render
alap fajlrendszere nem tartos, ezert eles naplozashoz csatolt persistent disk
es azon beluli utvonal, peldaul `FORECAST_DB_PATH=/var/data/forecasts.sqlite3`
szukseges. A lemez nem kapcsolodik be automatikusan, mert csak tamogatott,
fizetos szolgaltatasi csomaghoz adhato. Reszletek a
[Render persistent disk dokumentaciojaban](https://render.com/docs/disks) vannak.
