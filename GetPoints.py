from scipy.signal import find_peaks 
from datetime import datetime
import pandas_ta as ta
import pandas as pd



def AllNegZoneInfo(df):

    flag = False
    Open = 0
    Close = 0
    Low = 0
    Time = 0
    # ZoneRecords = []
    # ZoneFlag = False

    for i in reversed(range(len(df))):
        if flag == False:
            if (df['EndOfNegZone'][i] == True):
                flag = True
                Min = df['close'][i-1]

                for j in reversed(range(i)):
                    if df['MACD'][j] < 0:
                        # ZoneRecords.append(df['close'][j])
                        if Min > df['close'][j-1]:
                            Min = df['close'][j-1]
                            Low = df['low'][j-1]
                            Time = df['open_time'][j-2]
                            Open = df['open'][j-2]
                            Close = df['close'][j-2]
                            
                    else:
                        break
        else:
            break

    # if len(ZoneRecords) > 5:
    #     ZoneFlag = True
    return Open , Close , Low , Time 

############################################################################################

def AllPosZoneInfo(df):

    flag = False
    Open = 0
    Close = 0
    Time = 0
    High = 0
    # ZoneRecords = []
    # ZoneFlag = False

    for i in reversed(range(len(df))):
        if flag == False:
            if (df['EndOfPosZone'][i] == True):
                flag = True
                Max = df['close'][i-1]

                for j in reversed(range(i)):
                    if df['MACD'][j] > 0:
                        # ZoneRecords.append(df['close'][j])
                        if Max < df['close'][j-1]:
                            Max = df['close'][j-1]
                            Open = df['open'][j-2]
                            Close = df['close'][j-2]
                            High = df['high'][j-1]
                            Time = df['open_time'][j-2]
                    else:
                        break
        else:
            break
    # if len(ZoneRecords) > 5:
    #     ZoneFlag = True
    return Open , Close , High , Time 

###########################################################################################
def Calculate_MACD_CCI(df):

    # MACD
    macd = ta.macd(df['close'])
    df['MACD'] = macd['MACD_12_26_9']
    df['MACD-Lag1'] = df['MACD'].shift(1)
    df['MACD-Lag2'] = df['MACD'].shift(2)

    df['EndOfNegZone'] =  (df['MACD-Lag1'] < 0) & (df['MACD'] > 0) 
    df['EndOfPosZone'] =  (df['MACD-Lag1'] > 0) & (df['MACD'] < 0) 


    # CCI
    # cci = ta.cci(df['high'] , df['low'] , df['close'] , length=20)
    # df['CCI'] = cci
    # df['CCI-Lag1'] = df['CCI'].shift(1)
    # df['CrossUp'] = (df['CCI'] > 100) & (df['CCI-Lag1'])
    # df['CrossDown'] = (df['CCI'] < -100) & (df['CCI'] > -100)

    return df

############################################################################################

def SaveToList(p1 , p2):
    EntryPoint = []
    minPrice = min(p1,p2)
    maxPrice = max(p1,p2)
    EntryPoint.append(minPrice)
    EntryPoint.append(maxPrice)
    return EntryPoint



##############################################################################################

def LastNegZoneRecords(df):

    ZoneRecords = []
    flag = False
    for i in reversed(range(len(df))):
        if flag == False:
            if (df['EndOfNegZone'][i] == True):
                flag = True
                for j in reversed(range(i)):
                    if df['MACD'][j] < 0:
                        ZoneRecords.append(df['close'][j])
                    else:
                        break
        else:
            break

    if len(ZoneRecords) > 5:
        return True
    else:
        return False


##############################################################################################

def LastPosZoneRecords(df):

    ZoneRecords = []
    flag = False
    for i in reversed(range(len(df))):
        if flag == False:
            if (df['EndOfPosZone'][i] == True):
                flag = True
                for j in reversed(range(i)):
                    if df['MACD'][j] > 0:
                        ZoneRecords.append(df['close'][j])
                    else:
                        break
        else:
            break

    if len(ZoneRecords) > 5:
        return True
    else:
        return False

##############################################################################################

def LastNegZoneTime(df):

    flag = False
    Time = 0
    for i in reversed(range(len(df))):
        if flag == False:
            if (df['EndOfNegZone'][i] == True):
                flag = True
                Min = df['close'][i-1]
                Time = df['open_time'][i-2]

                for j in reversed(range(i)):
                    if df['MACD'][j] < 0:
                        if Min > df['close'][j-1]:
                            Min = df['close'][j-1]
                            Time = df['open_time'][j-1]
                    else:
                        break
        else:
            break             
    
    return Time


##############################################################################################

def LastPosZoneTime(df):

    Time = 0
    flag = False
    for i in reversed(range(len(df))):
        if flag == False:
            if (df['EndOfPosZone'][i] == True):
                flag = True
                Max = df['close'][i-1]
                Time = df['open_time'][i-2]

                for j in reversed(range(i)):
                    if df['MACD'][j] > 0:
                        if Max < df['close'][j-1]:
                            Max = df['close'][j-1]
                            Time = df['open_time'][j-1]
                    else:
                        break
        else:
            break
    
    return Time


##############################################################################################

def FindPivotsForLong(df):
# def LastSupplyExitPoint(df , t1 , t2):
    t1 = LastPosZoneTime(df)
    t2 = LastNegZoneTime(df)

    records = df.loc[(df['open_time'] >= t1) & (df['open_time'] <= t2)]
    # records = records['close'].to_list()

    openPrice = records['open'].to_list()
    closePrice = records['close'].to_list()

    maxPrice = []
    for i in range(len(openPrice)):
        if(closePrice[i] > openPrice[i]):
            maxPrice.append(closePrice[i])
        else:
            maxPrice.append(openPrice[i])

    peaks = find_peaks(maxPrice , height=0 , distance=5)
    peaksHeight = peaks[1]['peak_heights'].tolist()
    if not peaksHeight:
        peaksHeight = [0]

    peaksHeight.reverse()

    return peaksHeight


##############################################################################################

def FindPivotsForShort(df):
# def LastSupplyExitPoint(df , t1 , t2):
    t1 = LastPosZoneTime(df)
    t2 = LastNegZoneTime(df)

    records = df.loc[(df['open_time'] <= t1) & (df['open_time'] >= t2)]
    # records = records['close'].to_list()

    openPrice = records['open'].to_list()
    closePrice = records['close'].to_list()

    minPrice = []
    for i in range(len(openPrice)):
        if(closePrice[i] < openPrice[i]):
            minPrice.append(closePrice[i])
        else:
            minPrice.append(openPrice[i])
    
    valleys = Find_Valley(minPrice)

    if not valleys:
        valleys = [0]

    valleys.reverse()

    return valleys

##############################################################################################

def ConvertStrToData(str):
    t = datetime.strptime(str , '%Y-%m-%d %H:%M:%S')
    return t


##############################################################################################

def ConvertStrToDataHft(str):
    t = datetime.strptime(str , '%Y-%m-%d')
    return t


##############################################################################################

def ConvertStrToDataTradingView(str):
    t = datetime.strptime(str , '%Y-%m-%d %H:%M')
    return t
 
##############################################################################################

def CalculateDuration(t1 , t2):
    t1 = ConvertStrToData(t1)
    t2 = ConvertStrToData(t2)
    duration = t2 - t1
    return duration
    
##############################################################################################
 
def Find_Valley(nlist):
    valleys = []
    for idx in range(1, len(nlist) - 1):
        if nlist[idx + 1] > nlist[idx] < nlist[idx - 1]:
            valleys.append(nlist[idx])

    return valleys













































