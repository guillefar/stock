import os, sys, datetime as dt
import yfinance as yf
import pprint
symbols = os.getenv("SYMBOLS", "KRKNF,RR.L,1GOOGL.MI,XESC.DE,NBIS,RKLB,VUSA.AS,SEME.PA,DE000A2QP4B6.SG,RDDT,RGTI,IREN,POET,ASTS,AIR.PA,LITE,QQQ,VUAA.L,AMD,FSLR,ISUN.L,CSIQ,LYXIB.MC,AVGO,INRG.SW,AMPX").split(",")



for sym in symbols:
    dat=yf.Ticker(sym)
    print (sym)
    pprint.pprint(dat.info['symbol'])


#data = yf.download(symbols, period="2d", interval="1d", group_by="ticker", auto_adjust=False, threads=True)
