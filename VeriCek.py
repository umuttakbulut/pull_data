# -*- coding: utf-8 -*-
"""
Created on Sun May 22 19:55:28 2016
"""
import array, psycopg2, sys
import psycopg2.extras

from numpy import sin, pi
import datetime, time
import threading,socket

dbname = 'akimdb'
host = '192.168.34.241'
user = 'postgres'
password = '143053'

db_query = 'dbname=%s user=%s host=%s password=%s' %(dbname,user,host,password)

con = psycopg2.connect(db_query)
class ThreadZa (threading.Thread): 
    yeniveri = True
    tsayi = 0
    durum = 0
    def __init__(self, threadID): 
        threading.Thread.__init__(self) 
        self.threadID = threadID 
        self.con = con 
    def run(self):
        cur = con.cursor()
        self.durum = 1
        while self.tsayi>0 or self.yeniveri:
            self.yeniveri = False
            cur.execute('select fn_zamanasimi(1);')
            con.commit()
            row = cur.fetchone()
            self.tsayi = row[0]
            time.sleep(1)
        self.basladi = False
        self.durum = 2

class AnaMenu:
    th_za = ThreadZa(1)
    a_c_carpan = 1.5
    a_cerceve = 10
    a_tarama = 10
    a_onay_yuzde = 1000
    
    def referans_ekle(self):
        try:
            print u"DB baglantisi basarili"
            cur = con.cursor()
            dene = array.array('H')
            for i in range(0,300):
                dene.append(50 + round(30 * (sin(pi/150 * i))))
        
            self.sundur(dene, 100)
            cur.execute("INSERT INTO tblreferans (data,ornek,aciklama) VALUES (%s,300,'test')",(psycopg2.Binary(dene.tostring()),))
            con.commit()
            con.close()
            print "ekleme basarili"
        
        except psycopg2.DatabaseError, e:
            print 'Error %s' % e    
            
        finally:
            if con:
                con.close()
                
    def dinleme(self):
        cur_dinle = con.cursor()
        cur = con.cursor()
        cur_dinle.execute('LISTEN yeniveri;')
        cur_dinle.execute('LISTEN yenianaliz;')
        cur_dinle.execute('LISTEN otoanaliz_sorgu;')
        cur.execute('select fn_analiztamamla();');
        con.commit;
        self.th_za.start()
        print u"DB baglantisi basarili"       
        try:            
            while 1:
                state_veri = con.poll()
                if state_veri == psycopg2.extensions.POLL_OK:
                    if con.notifies:
                        notify = con.notifies.pop()
                        print "Gelen Kanal =", notify.channel
                        if notify.channel == "yeniveri":
                            self.analiz(notify.payload)
                        elif notify.channel == "yenianaliz":
                            cur.execute("select fn_analizekle(%s)" %(notify.payload))                            
                            if self.th_za.durum == 0:
                                self.th_za.start()
                            elif self.th_za.durum == 1:
                                self.th_za.yeniveri = True
                            else:
                                self.th_za = ThreadZa(1)
                                self.th_za.start()
                        elif notify.channel == "otoanaliz_sorgu":
                            cur.execute("notify otoanaliz_var,'%s';" %(socket.gethostname())) 
                time.sleep(0.1)
                                   
        except psycopg2.DatabaseError, e:
            print 'Error %s' % e    
            sys.exit(1)
    
    def analiz(self,veri):        
        onay = False
        cur = con.cursor(cursor_factory=psycopg2.extras.DictCursor)
        oran = 0 
        try:
            print "veri degeri=", veri    
            query = "SELECT * FROM tblveri where vno=%s" %(veri)   
            cur.execute(query)
            rows_veri = cur.fetchall()
            datav = array.array('H')
            
            for row_veri in rows_veri:
                if row_veri is None:
                    print "Yeni Veri Yok"
                    continue
                else:
                    del datav[:]
                    port_no = row_veri['fk_p_no']
                    veri_no = row_veri['vno']
                    datav.fromstring(row_veri['data'])
                    gelen_referans = self.analizYap(datav, oran, self.a_tarama, self.a_cerceve, self.a_c_carpan, port_no)
                    try:      
                        if gelen_referans['oran'] > self.a_onay_yuzde:
                            onay = True
                        
                        query = "INSERT INTO tblanaliz (fk_p_no,fk_v_no,fk_r_no,onay,oran,ts) VALUES (%s,%s,%s,%s,%s,%s)"
                        data = (port_no,veri_no, gelen_referans['rno'],onay, gelen_referans['oran'], datetime.datetime.now())
                        cur.execute(query,data)
                        con.commit()
                        
                        cur.execute("update tblveri set islendi = true where vno=%s" %(veri_no))
                        con.commit()
                                                            
                    except psycopg2.DatabaseError, e:
                        print 'Error %s' % e    
                        sys.exit(1)
                  
        except psycopg2.DatabaseError, e:
            print 'Error %s' % e    
            sys.exit(1)
       
    def analizYap(self,Data, oran, tarama, cerceve, hata, port_no):
        
        cur = con.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute('SELECT * FROM tblreferans where rno in (select rno from vportref where pno =%s)' %(port_no))
        rows_referans = cur.fetchall()
        
        datar = array.array('H')
        oran = 0;
        ilk = True  
        minfark=0
        try:
            alinan_id = 0
            for row_referans in rows_referans:
                del datar[:]
                datar.fromstring(row_referans['data'])
                
                for i in range(-1*tarama,tarama,1):  
                    veritoplam = 0
                    farktoplam = 0   
                    k = min(len(Data),len(datar))        
                    for j in range(0,k,1):
                        if ((i+j)>=0) and ((i+j)<k): 
                            if Data[j] > datar[i+j] * (1-cerceve/100) and Data[j] < datar[i+j] * (1+cerceve/100):
                                farktoplam +=  abs(Data[j] - datar[i+j]) # cercevenin ici
                            else:
                                farktoplam += int(abs(Data[j] - datar[i+j]) * hata)
                            veritoplam +=  Data[j]
                    if (ilk) or ((not ilk) and (farktoplam < minfark)):
                        minfark = farktoplam
                        alinan_id = row_referans['rno']
                        ilk = False
                        oran = 1000 - round((float(minfark) / float(veritoplam+1)) * 1000)    
            
        except psycopg2.DatabaseError, e:
            print 'Error %s' % e    
            sys.exit(1)
        result = {}
        result['oran'] = oran
        result['rno'] = alinan_id
        
        return result            
       
    def sundur(self, data):
        ei = 0
        size = 100
        esize = len(data)
        print "esize",esize
        datas = array.array('H')
        
        for i in range(0,size-1,1):
            ei = int(i*esize/size)
            if ei == 0:
                ei = 1
            elif ei == esize:
                ei = esize -1
            print ei
            
            datas.append((int(data[ei] + data[ei-1])/2))

        return datas
    
    def temizle(self):
        
        cur = con.cursor()
        try:
            cur.execute('DELETE FROM tblanaliz')
            con.commit()
            
            cur.execute('update tblveri set islendi=False')
            con.commit()
            
            print "basarili"
                    
        except psycopg2.DatabaseError, e:
            print 'Error %s' % e    
            sys.exit(1)
        

    def deger(self,data):
        print "data=",data
    
    
    def ayaroku(self):
        cur = con.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute('select * from tblayarlar')
        ayar = cur.fetchone()
        self.a_c_carpan = ayar['a_c_carpan']
        self.a_cerceve = ayar['a_cerceve']
        self.a_tarama = ayar['a_tarama']
        self.a_onay_yuzde = ayar['a_onay_yuzde']
        
    def analizkontrol(self):
        cur = con.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute('select vno from tblveri where islendi = false order by vno;')
        rows = cur.fetchall()
        for r in rows:
            print r
            self.analiz(r['vno']);
        con.commit()
        
c = AnaMenu()
con.set_isolation_level(0)
c.ayaroku()
c.analizkontrol()
c.dinleme()
