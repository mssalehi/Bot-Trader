import json
from datetime import datetime
import pandas as pd
import websocket
import GetPoints as gp
import requests
import pandas_ta as ta
import botHandler as bh

# Define daily and hourly dataframe
df_h = pd.DataFrame(columns=['open_time' , 'open' , 'high' , 'low' , 'close' , 'volume'])
df_d =  pd.DataFrame(columns=['open_time' , 'open' , 'high' , 'low' , 'close' , 'volume'])
last_candle = pd.DataFrame(columns=['open_time' , 'open' , 'high' , 'low' , 'close' , 'volume'])

# Create new DataFrame to log position information
long_log = pd.DataFrame(columns=['OpenTime' , 'OpenPrice' , 'ClosePrice' , 'CloseTime' , 'Profit' , 'Duration' , 'DemandPrice1' , 'DemandPrice2' , 'DemandTime', 'DemandStopLoss' , 'CloseReason'])
short_log = pd.DataFrame(columns=['OpenTime' , 'OpenPrice' , 'ClosePrice' , 'CloseTime' , 'Profit' , 'Duration' , 'SupplyPrice1' , 'SupplyPrice2' , 'SupplyTime', 'SupplyStopLoss' , 'CloseReason'])


# Socket for get 1h_timeframe candle
symbol = 'btcusdt'
interval_h = '1h'
socket = f'wss://fstream.binance.com/ws/{symbol}@kline_{interval_h}'

# Api for get 1d_timeframe candle
interval_d='1d'
limit = 2
base_url = 'https://fapi.binance.com'
url = f'{base_url}/fapi/v1/klines?symbol={symbol}&interval={interval_d}&limit={limit}'

# GetTrend Value
LastStatus = 'short'

# Long Position Variable
LongEntryPoints = []
LongExitPoints = []
LongDemandTime = []
LongStopLoss = []
Lcounter = 0 
LongZoneChecked = False
LongOpenPos = False
long_lastIndex = 0
long_lastOpenTime = 0   
long_lastOpenPrice = 0
long_lastDemandPrice1 = 0
long_lastDemandPrice2 = 0
long_lastDemandTime = 0
long_lastStopLoss = 0
long_sl = 0

# Short Position Variable
ShortEntryPoints = []
ShortExitPoints = []
ShortSupplyTime = []
ShortStopLoss = []
Scounter = 0
ShPosNum = 0
ShortZoneChecked = False
ShortOpenPos = False
short_lastIndex = 0
short_lastOpenTime = 0   
short_lastOpenPrice = 0
short_lastSupplyPrice1 = 0
short_lastSupplyPrice2 = 0
short_lastSupplyTime = 0
short_lastStopLoss = 0






def on_open(ws):
    print('Socket is Started!')



def on_message(ws , message):
    data = json.loads(message)
    candle = data['k']
    if candle['x'] == True:
        # Save candle in dataframe
        last_candle.loc[0] = {'open_time' : str(datetime.utcfromtimestamp(int(str(candle['t'])[:-3]))) , 'open' : float(candle['o']) , 'high' : float(candle['h']) , 'low' : float(candle['l']) , 'close' : float(candle['c']) , 'volume' : float(candle['v']) }
        global df_h
        df_h = pd.concat([df_h , last_candle] , ignore_index=True)


        # Check the change of day for get daily candle
        current_time = gp.ConvertStrToData(df_h['open_time'][-1:].values[0])
        if current_time.hour == 1:
            daily_candle = requests.get(url).text
            daily_candle = json.loads(daily_candle)
            df_d.loc[len(df_d.index)] = {'open_time' : str(datetime.utcfromtimestamp(int(str(daily_candle[-2:-1][0][0])[:-3]))) , 'open' : daily_candle[-2:-1][0][1] , 'high' : daily_candle[-2:-1][0][2] , 'low' : daily_candle[-2:-1][0][3] , 'close' : daily_candle[-2:-1][0][4] , 'volume' : daily_candle[-2:-1][0][5] }
        
        # Calculate GetTrend
        global LastStatus
        if (current_time.hour == 1) & (len(df_d) > 2):
            if (df_d['close'][-2:-1].values[0] < df_d['close'][-1:].values[0]):
                LastStatus = 'long'
                bh.sendMessage(f"{df_h['open_time'][-1:].values[0]}\nGet Trend Signal : {LastStatus}")
            elif (df_d['close'][-2:-1].values[0] > df_d['close'][-1:].values[0]):
                LastStatus = 'short'
                bh.sendMessage(f"{df_h['open_time'][-1:].values[0]}\nGet Trend Signal : {LastStatus}")



        if len(df_h) >= 34:
            # Calculate MACD indicator
            macd = ta.macd(df_h['close'])
            df_h['MACD'] = macd['MACD_12_26_9']
            df_h['MACD-Lag1'] = df_h['MACD'].shift(1)
            df_h['MACD-Lag2'] = df_h['MACD'].shift(2)
            df_h['EndOfNegZone'] =  (df_h['MACD-Lag1'] < 0) & (df_h['MACD'] > 0) 
            df_h['EndOfPosZone'] =  (df_h['MACD-Lag1'] > 0) & (df_h['MACD'] < 0) 



    #############################################  Long Position    ################################################

            # Find Demand area
            global LongZoneChecked
            if df_h['EndOfNegZone'][-1:].values[0] == True:
                if (gp.LastNegZoneRecords(df_h) == True) & (gp.LastPosZoneRecords(df_h) == True):
                    p1 , p2 , lw , td  = gp.AllNegZoneInfo(df_h)
                    LongEntryPoints.append(gp.SaveToList(p1 , p2))
                    LongDemandTime.append(td)
                    LongExitPoints.append(gp.FindPivotsForLong(df_h))
                    LongStopLoss.append(lw)
                    LongZoneChecked = True


            # Open long position
            global LongOpenPos , long_lastIndex
            if (LongZoneChecked == True) and (LongOpenPos == False) and (df_h['MACD'][-1:].values[0] < 0) and (LastStatus == 'long'):
                for j in range(len(LongEntryPoints)):
                    if (LongEntryPoints[j][0] < df_h['close'][-1:].values[0] < LongEntryPoints[j][1]):
                        if (LongExitPoints[j] != [0]):
                            long_lastOpenTime = df_h['open_time'][-1:].values[0]
                            long_lastOpenPrice = df_h['close'][-1:].values[0]
                            long_lastDemandPrice1 = LongEntryPoints[j][0]
                            long_lastDemandPrice2 = LongEntryPoints[j][1]
                            long_lastDemandTime = LongDemandTime[j]
                            long_lastStopLoss = LongStopLoss[j]
                            long_log.loc[long_lastIndex] = [long_lastOpenTime, long_lastOpenPrice , 0 , 0 , 0 , 0 , long_lastDemandPrice1 , long_lastDemandPrice2 , long_lastDemandTime , long_lastStopLoss , 0]
                            long_tp = long_lastOpenPrice + ((long_lastOpenPrice * 10) / 100 )
                            long_sl = LongStopLoss[j]
                            Lcounter = j
                            LongOpenPos = True
                            bh.sendMessage(f"Long Position has opend at {df_h['open_time'][-1:].values[0]}")

                            break

            # Close long position
            elif (LongOpenPos == True):

                # Close by GetTrend
                if LastStatus == 'short':
                        profit = ((df_h['close'][-1:].values[0] - long_lastOpenPrice) / long_lastOpenPrice) * 100
                        duration = gp.CalculateDuration(long_lastOpenTime , df_h['open_time'][-1:].values[0])
                        long_log.loc[long_lastIndex] = [long_lastOpenTime , long_lastOpenPrice , df_h['close'][-1:].values[0] , df_h['open_time'][-1:].values[0] , profit , duration , long_lastDemandPrice1 , long_lastDemandPrice2 , long_lastDemandTime , long_lastStopLoss ,'GetTrend!']
                        LongPosNum = LongPosNum + 1
                        long_log.to_csv('BTCLongPositions.csv')
                        LongOpenPos = False
                        long_lastIndex+=1
                        bh.sendMessage(f"Long Position has closed at {df_h['open_time'][-1:].values[0]}\n reason : Get Trend")


                else:

                    # Close on StopLoss
                    if (df_h['close'][-1:].values[0] <= long_sl) :
                        profit = ((long_sl - long_lastOpenPrice) / long_lastOpenPrice) * 100
                        duration = gp.CalculateDuration(long_lastOpenTime , df_h['open_time'][-1:].values[0])
                        LongTotalFund = LongTotalFund + ((profit * LongTotalFund) / 100)
                        long_log.loc[long_lastIndex] = [long_lastOpenTime , long_lastOpenPrice , long_sl , df_h['open_time'][-1:].values[0] , profit , duration , long_lastDemandPrice1 , long_lastDemandPrice2 , long_lastDemandTime , long_lastStopLoss , 'StopLoss!']
                        LongOpenPos = False
                        LongPosNum = LongPosNum + 1
                        long_log.to_csv('BTCLongPositions.csv')
                        LongEntryPoints.pop(Lcounter)
                        LongExitPoints.pop(Lcounter)
                        LongStopLoss.pop(Lcounter)
                        LongDemandTime.pop(Lcounter)
                        long_lastIndex+=1
                        bh.sendMessage(f"Long Position has closed at {df_h['open_time'][-1:].values[0]}\n reason : Stop Loss")



                    # Close on TakeProfit
                    elif (df_h['close'][-1:].values[0] > long_tp):
                        profit = ((long_tp - long_lastOpenPrice) / long_lastOpenPrice) * 100
                        duration = gp.CalculateDuration(long_lastOpenTime , df_h['open_time'][-1:].values[0])
                        LongTotalFund = LongTotalFund + ((profit * LongTotalFund) / 100)
                        long_log.loc[long_lastIndex] = [long_lastOpenTime , long_lastOpenPrice , long_tp , df_h['open_time'][-1:].values[0] , profit , duration , long_lastDemandPrice1 , long_lastDemandPrice2 , long_lastDemandTime , long_lastStopLoss , 'TakeProfit!']
                        LongPosNum = LongPosNum + 1
                        long_log.to_csv('BTCLongPositions.csv')
                        LongOpenPos = False
                        long_lastIndex +=1
                        bh.sendMessage(f"Long Position has closed at {df_h['open_time'][-1:].values[0]}\n reason : Take Profit")



    #############################################  Short Position    ################################################


            # Find supply area
            global ShortZoneChecked
            if df_h['EndOfPosZone'][-1:].values[0] == True:
                if (gp.LastPosZoneRecords(df_h) == True) & (gp.LastNegZoneRecords(df_h) == True) :
                    p1 , p2 , hg , ts = gp.AllPosZoneInfo(df_h)
                    ShortEntryPoints.append(gp.SaveToList(p1 , p2))
                    ShortSupplyTime.append(ts)
                    ShortExitPoints.append(gp.FindPivotsForShort(df_h))
                    ShortStopLoss.append(hg)
                    ShortZoneChecked = True

            # Open short position
            global ShortOpenPos , short_lastIndex
            if (ShortZoneChecked == True) & (ShortOpenPos == False) & (df_h['MACD'][-1:].values[0] > 0)  & (LastStatus == 'short'):
                for j in range(len(ShortEntryPoints)):
                    if (ShortEntryPoints[j][0] < df_h['close'][-1:].values[0] < ShortEntryPoints[j][1]):
                        if (ShortExitPoints[j] != [0]):
                            short_lastOpenTime = df_h['open_time'][-1:].values[0]
                            short_lastOpenPrice = df_h['close'][-1:].values[0]
                            short_lastSupplyPrice1 = ShortEntryPoints[j][0]
                            short_lastSupplyPrice2 = ShortEntryPoints[j][1]
                            short_lastSupplyTime = ShortSupplyTime[j] 
                            short_lastStopLoss = ShortStopLoss[j]
                            short_log.loc[short_lastIndex] = [short_lastOpenTime, short_lastOpenPrice , 0 , 0 , 0 , 0 , short_lastSupplyPrice1 , short_lastSupplyPrice2 , short_lastSupplyTime , short_lastStopLoss , 0]
                            short_tp = short_lastOpenPrice - ((short_lastOpenPrice * 10) / 100 )
                            short_sl = ShortStopLoss[j]
                            ShortOpenPos = True
                            Scounter = j
                            bh.sendMessage(f"Short Position has opend at {df_h['open_time'][-1:].values[0]}")

                            break



            # Close short position
            elif (ShortOpenPos == True):

                # Close by GetTrend
                if LastStatus == 'long':
                        profit = ((df_h['close'][-1:].values[0] - short_lastOpenPrice) / short_lastOpenPrice) * 100 * (-1)
                        duration = gp.CalculateDuration(short_lastOpenTime , df_h['open_time'][-1:].values[0])
                        ShortTotalFund = ShortTotalFund + ((profit * ShortTotalFund) / 100)
                        short_log.loc[short_lastIndex] = [short_lastOpenTime , short_lastOpenPrice , df_h['close'][-1:].values[0] , df_h['open_time'][-1:].values[0] , profit , duration , short_lastSupplyPrice1 , short_lastSupplyPrice2 , short_lastSupplyTime , short_lastStopLoss , 'GetTrend']
                        ShPosNum = ShPosNum + 1
                        short_log.to_csv('BTCShortPositions.csv')
                        ShortOpenPos = False
                        short_lastIndex+=1
                        bh.sendMessage(f"Short Position has closed at {df_h['open_time'][-1:].values[0]}\n reason : Get Trend")

                else:

                    # Close on StopLoss
                    if (df_h['close'][-1:].values[0] >= short_sl) :
                        profit = (((short_sl - short_lastOpenPrice) / short_lastOpenPrice) * 100) * (-1)
                        duration = gp.CalculateDuration(short_lastOpenTime , df_h['open_time'][-1:].values[0])
                        ShortTotalFund = ShortTotalFund + ((profit * ShortTotalFund) / 100)
                        short_log.loc[short_lastIndex] = [short_lastOpenTime , short_lastOpenPrice , short_sl , df_h['open_time'][-1:].values[0] , profit , duration , short_lastSupplyPrice1 , short_lastSupplyPrice2 , short_lastSupplyTime , short_lastStopLoss , 'StopLoss']
                        ShortOpenPos = False
                        ShPosNum = ShPosNum + 1
                        short_log.to_csv('BTCShortPositions.csv')
                        ShortEntryPoints.pop(Scounter)
                        ShortExitPoints.pop(Scounter)
                        ShortStopLoss.pop(Scounter)
                        ShortSupplyTime.pop(Scounter)
                        short_lastIndex+=1
                        bh.sendMessage(f"Short Position has closed at {df_h['open_time'][-1:].values[0]}\n reason : Stop Loss")


                    # Close on TakeProfit
                    elif (df_h['close'][-1:].values[0] < short_tp):
                        profit = (((short_tp - short_lastOpenPrice) / short_lastOpenPrice) * 100) * (-1)
                        duration = gp.CalculateDuration(short_lastOpenTime , df_h['open_time'][-1:].values[0])
                        ShortTotalFund = ShortTotalFund + ((profit * ShortTotalFund) / 100)
                        short_log.loc[short_lastIndex] = [short_lastOpenTime , short_lastOpenPrice , short_tp , df_h['open_time'][-1:].values[0] , profit , duration , short_lastSupplyPrice1 , short_lastSupplyPrice2 , short_lastSupplyTime , short_lastStopLoss , 'TakeProfit']
                        ShPosNum = ShPosNum + 1
                        short_log.to_csv('BTCShortPositions.csv')
                        ShortOpenPos = False
                        short_lastIndex+=1
                        bh.sendMessage(f"Short Position has closed at {df_h['open_time'][-1:].values[0]}\n reason : Take Profit")


        else:
            print('Can not Calculate MACD!')


def on_close(ws):
    print('Socket is Ended!')



ws = websocket.WebSocketApp(socket , on_open=on_open , on_message=on_message , on_close=on_close)

ws.run_forever()
