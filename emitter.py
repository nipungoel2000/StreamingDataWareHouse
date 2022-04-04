import time
import random
import csv

SLEEP_TIME = 5
EMITTER_LOC = "fact_data_emitter/"

def emitter(cnt):
    storeId = random.randint(1,4)
    productId = random.randint(1,5)
    purchaseAmount = random.randint(1,5000)
    data = [storeId, productId, purchaseAmount]
    # data = [{"storeId" : storeId, "productId" : productId, "purchaseAmount" : purchaseAmount}]
    fileName = EMITTER_LOC + str(cnt) + ".csv"
    with open(fileName, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(data)

if __name__=="__main__":
    cnt = 1
    while True:
        print(str(cnt) + " file created..")
        emitter(cnt)
        cnt += 1
        time.sleep(SLEEP_TIME)
