import time
import random
import csv

SLEEP_TIME = 1
EMITTER_LOC = "fact_data_emitter/"


def emitter():
    cnt = 1
    lst = []
    while True:
        storeId = random.randint(1, 4)
        productId = random.randint(1, 5)
        while((storeId, productId) in lst):
            storeId = random.randint(1, 4)
            productId = random.randint(1, 5)
        lst.append((storeId, productId))
        purchaseAmount = random.randint(1, 5000)
        data = [storeId, productId, purchaseAmount]
        # print(data)
        # data = [{"storeId" : storeId, "productId" : productId, "purchaseAmount" : purchaseAmount}]
        fileName = EMITTER_LOC + str(cnt) + ".csv"
        with open(fileName, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(data)
        print(str(cnt) + " file created..")
        cnt += 1
        time.sleep(SLEEP_TIME)


if __name__ == "__main__":
    # cnt = 1
    # while True:
    # print(str(cnt) + " file created..")
    time.sleep(5)
    emitter()
    # cnt += 1
    #
