import pandas as pd
import matplotlib.pyplot as plt
import requests
from bs4 import BeautifulSoup
import sqlite3

#plt.rcParams['font.family']='Microsoft YaHei'
plt.rcParams['font.family']=['Arial Unicode MS']
parameters = {'xtick.labelsize': 15,'ytick.labelsize': 20,
          'axes.titlesize': 25,
          'figure.titlesize':30,
          'legend.fontsize':12}
plt.rcParams.update(parameters)

def create_URLs(county):
    url_list=[]
    for i in ['108','109','110','111']:
        for j in ['1','2','3','4']:
            
            url = r'/Users/miao/Downloads/房價{0}年第{1}季/{2}_lvr_land_a.csv'
            url = url.format(i,j,county)
            url_list.append(url)
          
    url_11105 = r'/Users/miao/Downloads/房價111年第5季/{0}_lvr_land_a.csv'
    url_11105 = url_11105.format(county.lower())
    url_list.append(url_11105)
    return url_list


def readfile(url_list): 
    dfs = pd.DataFrame()  
    for i in range(len(url_list)):
        df = pd.read_csv(url_list[i],       
                         usecols=['交易年月日','鄉鎮市區','土地位置建物門牌','交易標的','總價元','單價元平方公尺'],
                         skiprows=([1])).iloc[::-1]
        df.dropna(subset='單價元平方公尺',inplace=True)
        dropvalues=['土地','建物','車位']
        df = df[df.交易標的.isin(dropvalues) == False]
        df['交易年月日'] = df['交易年月日'].astype(str)
        #用正則表達式篩選格式錯誤的年月
        df = df[df['交易年月日'].str.match('^1(0[89]|1[01])(0[1-9]|1[0-2])')]
        df['交易年月日'] = df['交易年月日'].str[:5]
        dfs = pd.concat([dfs,df],axis=0, ignore_index=True) 
    
    #平方公尺換成坪 *3.3058
    dfs['單價元平方公尺'] = dfs['單價元平方公尺']*3.3058
    dfs.rename(columns = {'單價元平方公尺':'單價元每坪'},inplace = True)
    return dfs


def drop_outliners(dfs,county): 
    des = dfs.describe()
    print('---未去除離群值---')
    print(des)
    mean = des.loc['mean']['單價元每坪']
    std = des.loc['std']['單價元每坪']
    #作圖
    fig1 = plt.figure()
    plt.hist(dfs['單價元每坪'], bins=5000, range = [0, 2000000],label= county)
    plt.xlabel('每坪單價(百萬元)')
    plt.legend()
    plt.show()
    
    #用未開平方根的原始資料取標準差會拿到每坪單價0元的資料   
    #mask = (dfs['單價元平方公尺']>=mean-2*std)&(dfs['單價元平方公尺']<=mean+2*std)
    #dfs1 = dfs[mask]
    
    #將單價元每坪資料取平方根得到平均值與標準差
    sqrt_dfs = dfs['單價元每坪']**(1/2)
    sqrt_des = sqrt_dfs.describe()
    print('---取平方根後---')
    print(sqrt_des)
    sqrt_mean = sqrt_des.loc['mean']
    sqrt_std = sqrt_des.loc['std']
    #取3個標準差內的值並移除離群值 
    sqrt_mask = (sqrt_dfs.values>=sqrt_mean-3*sqrt_std)&(sqrt_dfs.values<=sqrt_mean+3*sqrt_std)
    dfs1 = dfs[sqrt_mask]   
    des1 = dfs1.describe()
    print('---去除離群值---')
    print(des1)    
    #作圖
    fig2 = plt.figure()
    plt.hist(dfs1['單價元每坪'], bins=5000,range = [0, 2000000],label= county)
    plt.xlabel('每坪單價(百萬元)')
    plt.legend()
    plt.show()
    return dfs1


def groupby_area(dfs2,county):    
    aggregation_params = {'單價元每坪': 'mean','交易年月日':'count'}
    groupby_dfs2 = dfs2.groupby('鄉鎮市區').agg(aggregation_params) 
    groupby_dfs2['區域名']= county+groupby_dfs2.index
    groupby_dfs2 = groupby_dfs2.rename(columns = {'交易年月日':'成交筆數'})
    groupby_dfs2 = groupby_dfs2.rename(columns = {'單價元每坪':'平均每坪單價'})
    groupby_dfs2 = groupby_dfs2.sort_values(['平均每坪單價'],ascending=False)         
    print(groupby_dfs2)
    return groupby_dfs2


def groupby_date(dfs1):
    aggregation_params = {'單價元每坪': 'mean', '總價元': 'sum','交易年月日':'count'}
    groupby_dfs1 = dfs1.groupby(dfs1['交易年月日']).aggregate(aggregation_params)
    groupby_dfs1 = groupby_dfs1.rename(columns = {'交易年月日':'成交筆數'})
    groupby_dfs1 = groupby_dfs1.rename(columns = {'單價元每坪':'平均每坪單價'}) 
    return groupby_dfs1


def crawl_interest():
    re = requests.get('https://www.cbc.gov.tw/tw/lp-640-1-1-20.html')
    print(re.status_code)
    soup = BeautifulSoup(re.text,'lxml')
    rawdata = soup.find_all('tr')
    my_list=[]
    for i in range(len(rawdata)-1,1,-1):
        date = rawdata[i].find('td',{'data-th':'標題(日期)'}).text
        interest = rawdata[i].find('td',{'data-th':'重貼現率'}).text
        my_list.append([date,interest])
    interest_data = pd.DataFrame(my_list, columns=['日期','重貼現率'])
    interest_data['重貼現率'] = pd.to_numeric(interest_data['重貼現率'])
    interest_data['西元年'] = [i.split('/')[0] for i in interest_data['日期']]
    interest_data['月份'] = [i.split('/')[1] for i in interest_data['日期']]
    for i in range(len(interest_data['月份'].values)):
        if int(interest_data['月份'].values[[i]]) < 10:
            interest_data['月份'].values[[i]] = '0'+ interest_data['月份'].values[[i]]       
    interest_data['民國年'] = [str(int(i)-1911) for i in interest_data['西元年']]
    interest_data['民國年月份']= interest_data['民國年'].values + interest_data['月份'].values   
    return interest_data

def read_bankinterest():
    data = pd.read_csv('重貼現率.csv')
    return data

def read_houseinterest():
    houseinterest = pd.read_csv('五大行庫平均房貸利率.csv',header=0).iloc[::-1]
    houseinterest['年度'] = [houseinterest['時間'][i].split('/')[0] for i in range(len(houseinterest)-1,-1,-1)]
    houseinterest['月份'] = [houseinterest['時間'][i].split('/')[1] for i in range(len(houseinterest)-1,-1,-1)]
    houseinterest['年度月份']=  houseinterest['年度'].values +  houseinterest['月份'].values
    return houseinterest

def write_toSQLite(county_df):
    #county_df寫入資料庫
    conn = sqlite3.connect('county_df.db')
    cursor = conn.cursor()
    sql='CREATE TABLE IF NOT EXISTS county(平均每坪單價,總價元,成交筆數,縣市,日期)'
    cursor.execute(sql)
    conn.commit()
    county_df.to_sql('county',conn,if_exists='replace',index=False)
    county = pd.read_sql('SELECT * FROM county',conn)
    conn.close()
    return county

#Main
#依照縣市代號來讀檔案:'台南市':'D','桃園市':'H'
county_df = pd.DataFrame()
county_dict={'台北市':'A','新北市':'F','桃園市':'H','台中市':'B','台南市':'D','高雄市':'E'}
for i in county_dict:   
    county_urllist = create_URLs(county_dict[i])
    result_dfs = readfile(county_urllist)
    result_dfs1 = drop_outliners(result_dfs,i)
    #groupby_area_data = groupby_area(result_dfs1,i)  
    #寫檔for畫地圖用
    #groupby_area_data.to_csv('locations.csv',mode='a',header=['平均每坪單價','成交筆數','區域名'],index=False)   
    groupby_date_data = groupby_date(result_dfs1) 
    groupby_date_data = groupby_date_data.drop(groupby_date_data.tail(1).index)
    
    #作圖:平均每坪單價折線圖及成交筆數長條圖
    fig, ax1 = plt.subplots(figsize=(25,8))
    ax2 = ax1.twinx()
    curve1 = ax1.bar(groupby_date_data.index, groupby_date_data['成交筆數'], label="成交筆數", color='red',alpha=0.5)
    curve2 = ax2.plot(groupby_date_data.index, groupby_date_data['平均每坪單價'], label="平均每坪單價", marker='.', color='b',linewidth='0.8')
    ax2.yaxis.set_major_formatter(plt.FuncFormatter(lambda value, pos: f'{value/10000:.0f}萬'))
    ax1.set_xticks(groupby_date_data.index[::2])
    #for a,b in zip(x,y2):
        #ax2.text(a, b, b, ha='center',va='bottom',fontsize=8)
    ax1.set_ylim(0,6000)
    ax1.legend(loc='upper left')
    ax2.legend(loc='upper right')
    ax1.set_xlabel('年度月份',fontsize=20)
    plt.title('%s平均每坪單價及成交筆數'%(i))
    plt.grid(axis='y')
    plt.show()
    fig.savefig('%s平均每坪單價及成交筆數.jpg'%(i))
    #組合    
    groupby_date_data['縣市']= i
    groupby_date_data['日期']= groupby_date_data.index
    county_df = pd.concat([county_df,groupby_date_data],axis=0,ignore_index=True)

#寫入資料庫
county = write_toSQLite(county_df)
county.to_csv('county.csv')
#爬央行利率網頁    
interest_data = crawl_interest()
data = read_bankinterest()
#讀取五大行庫平均房貸利率
houseinterest = read_houseinterest()


#作圖:年度對平均房貸利率及重貼現率走勢折線圖
plt.figure(figsize=(25,8))
plt.plot(houseinterest['年度月份'].values,houseinterest['五大行庫平均房貸利率(%)'].values,label='平均房貸利率(%)',marker='.')
plt.plot(data.index,data['重貼現率'].values,label='重貼現率(%)',marker='.')
plt.xticks(houseinterest.年度月份[::2])
plt.xlabel('年度月份')
plt.ylabel('利率(%)')
plt.title('年度對平均房貸利率及重貼現率走勢')
plt.legend()
plt.grid(axis='y')
plt.show()



#作圖:成交量圓餅圖
totalnum = groupby_date_data['成交筆數'].sum()
print(totalnum)
piefig = plt.figure(figsize=(25,8))
numbers = [79024,179008,143532,126626]
county = ['台北市','新北市','台中市','高雄市']
colors=['lightgreen','lightblue','yellow','pink']
explode=[0.1,0,0,0]
plt.pie(numbers, labels=county, colors=colors, shadow=True,
                         autopct='%1.1f%%',explode=explode,startangle=90,
                         textprops={'fontsize':12,'color':'k'})
plt.axis('equal')
plt.title('108年-111年總成交筆數')
plt.show()
plt.savefig('108年-111年總成交筆數圓餅圖.jpg')

#作圖:各縣市平均每坪單價及利率折線圖
fig, ax1 = plt.subplots(figsize=(25,8))
ax2 = ax1.twinx()
curve1 = ax1.scatter(data.index, data['重貼現率'], label='重貼現率',alpha=0.5)
curve2 = ax2.plot(county.index[0:48], county['平均每坪單價'][0:48].values, label='台北市', marker='.',linewidth='0.8')
curve3 = ax2.plot(county.index[0:48], county['平均每坪單價'][48:96].values, label='台中市', marker='.',linewidth='0.8')
curve4 = ax2.plot(county.index[0:48], county['平均每坪單價'][96:144].values, label='高雄市', marker='.',linewidth='0.8')
curve5 = ax2.plot(county.index[0:48], county['平均每坪單價'][144:192].values, label='新北市', marker='.',linewidth='0.8')
ax2.yaxis.set_major_formatter(plt.FuncFormatter(lambda value, pos: f'{value/10000:.0f}萬'))
ax2.set_xticks(county.index[0:48:2])
for a,b in zip(data['民國年月份'],data['重貼現率']):
    ax1.text(a, b,data['日期'].values, ha='center',va='bottom',fontsize=8)
ax1.legend(loc='upper left')
ax2.legend(loc='upper right')
ax1.set_xlabel('年度月份')
plt.title('各縣市平均每坪單價與利率走勢')
plt.show()
fig.savefig('各縣市平均每坪單價與利率走勢.jpg')


#作圖:各縣市平均每坪單價折線圖
plt.figure(figsize=(25,8))
plt.plot(county_df.index[0:47],county_df['平均每坪單價'][0:47].values, label='台北市', marker='.')
plt.plot(county_df.index[0:47],county_df['平均每坪單價'][47:94].values, label='新北市', marker='.')
plt.plot(county_df.index[0:47],county_df['平均每坪單價'][141:188].values, label='台中市', marker='.')
plt.plot(county_df.index[0:47],county_df['平均每坪單價'][235:].values, label='高雄市', marker='.')
ticks = range(0,47,2)
labels=[county_df['日期'][i] for i in range(0,47,2)]
plt.xticks(ticks,labels)
plt.xlabel('年度月份')
plt.ylabel('平均每坪單價')
plt.legend()
plt.grid(axis='y')
plt.show()