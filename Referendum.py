#!/apps/anaconda-2.3.0/bin/python

from referendum_daily import *

def Detect(l):
    try:
        return detect(l.decode('utf-8'))
    except:
        return 'Not a language'
    
if __name__ == "__main__":

    Bastaunsi = convert_data('bastaunsi')
    Iovotono = convert_data('iovotono')

    Bastaunsi = Bastaunsi.dropna()
    Iovotono = Iovotono.dropna()
    
    pool = Pool(processes=8)
    
    bastaunsi_It = pool.map(Detect,Bastaunsi['text'])
    Temp = []
    for l in bastaunsi_It:
        Temp.append(l)
    Bastaunsi['lan'] = Temp
    
    iovotono_It = pool.map(Detect,Iovotono['text'])
    Temp = []
    for l in iovotono_It:
        Temp.append(l)
    Iovotono['lan'] = Temp
    
    Bastaunsi_It = Bastaunsi[Bastaunsi['lan'] == 'it']
    Iovotono_It = Iovotono[Iovotono['lan'] == 'it']
    
    Bastaunsi_It.to_csv('Bastaunsi_It.csv',encoding = 'utf-8')
    Iovotono_It.to_csv('Iovotono_It.csv',encoding = 'utf-8')
    
    