import module
from queue import Queue
insertion_q=Queue(maxsize=0)
deletion_q=Queue(maxsize=0)
tick=0

def ETL(paths):
    parser=module.XMLparser()
    params=parser.getWindowparams()
    wsize=params["window_size"]
    wvel=params["window_velocity"]
    sz=insertion_q.qsize()
    if tick==0:
        insertion_q.put(paths[0])
        if insertion_q.qsize()==wsize:
            # call aravind function on insertion_q
            while insertion_q.qempty==False:
                deletion_q.put(insertion_q.get())
            tick=tick+1
    else:
        insertion_q.put(paths[0])
        if insertion_q.qsize()==wvel:
            while insertion_q.qempty==False:
                deletion_q.get()
                deletion_q.put(insertion_q.get())
            # call aravind function on deletion_q
            tick=tick+1