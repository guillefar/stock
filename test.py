import os, sys, datetime as dt
import yfinance as yf
import pprint
#symbols = os.getenv("SYMBOLS", "KRKNF,RR.L,1GOOGL.MI,XESC.DE,NBIS,RKLB,VUSA.AS,SEME.PA,DE000A2QP4B6.SG,RDDT,RGTI,IREN,POET,ASTS,AIR.PA,LITE,QQQ,VUAA.L,AMD,FSLR,ISUN.L,CSIQ,LYXIB.MC,AVGO,INRG.SW,AMPX").split(",")
symbols = os.getenv("SYMBOLS", "MDT,SMCI,AUR,LDEU.L,HEDJ.MI,CHGX,FIX,GOP,NANC,TE,IQSA.DE,GERD.SW,CNRG,VUG,EXUS.DE,VWRL.AS,IQQH.DE,GCLE.MI,MU,CLS,APLD,AVAV,PYPL,^STOXX50E,BBVAE.MC,SPY").split(",")
#symbols = os.getenv("SYMBOLS", "LDEU.L").split(",")

for sym in symbols:
    print (sym)
    dat=yf.Ticker(sym)
 #
 #
 # ["Close"]   pprint.pprint(dat.info)

    pprint.pprint(dat.info['symbol'])
    pprint.pprint(dat.fast_info["currency"])
    pprint.pprint(dat.fast_info["last_price"])

    pprint.pprint(dat.info['longName'])
#    pprint.pprint(dat.info['longBusinessSummary'])
    print ("----")


#data = yf.download(symbols, period="2d", interval="1d", group_by="ticker", auto_adjust=False, threads=True)
