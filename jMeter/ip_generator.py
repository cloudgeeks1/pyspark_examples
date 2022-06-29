import csv
import datetime
import os
import random
import socket
import struct

os.remove('/Users/pratikjoshi/PycharmProjects/cloudgeeks_pyspark_examples/jMeter/ip_load_test_data.csv')

i = 0
with open('ip_load_test_data.csv', 'w', encoding='UTF8') as f:
    writer = csv.writer(f)

    for n in range(20000, 0xffffffff, 333):
        list_data = []
        list_data.append(socket.inet_ntoa(struct.pack('>I', n)))
        list_data.append('SID' + '_' + str(random.randint(0, 9)))
        list_data.append('TFID' + '_' + str(random.randint(10, 19)))
        list_data.append('DVC_ID' + '_' + str(random.randint(20, 29)))
        list_data.append(random.choice(['USA', 'IND', 'LON', 'NYC', 'AUS', 'CHI', 'JAP', 'UKR', 'RUS', 'SRK']))
        list_data.append('CTE' + '_' + str(random.randint(40, 49)))
        list_data.append('BCR' + '_' + str(random.randint(50, 59)))
        list_data.append('PGM' + '_' + str(random.randint(60, 69)))
        list_data.append('EPI' + '_' + str(random.randint(70, 79)))
        list_data.append('CMP_ID' + '_' + str(random.randint(80, 89)))
        list_data.append('PUB_ID' + '_' + str(random.randint(90, 99)))
        list_data.append('PUB_NM' + '_' + str(random.randint(100, 109)))
        list_data.append('AP_ID' + '_' + str(random.randint(110, 119)))
        list_data.append('AP_NM' + '_' + str(random.randint(120, 129)))
        list_data.append('PLC_ID' + '_' + str(random.randint(130, 139)))
        list_data.append('POD' + '_' + str(random.randint(140, 149)))
        list_data.append('CST_1' + '_' + str(random.randint(150, 159)))
        list_data.append('CST_2' + '_' + str(random.randint(160, 169)))
        list_data.append(datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f"))
        writer.writerow(list_data)
        i = i + 1
        if i >= 40000:
            break
            break