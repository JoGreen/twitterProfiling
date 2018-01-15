import os

def inizializing(restart):
    # type:(bool)->(set([str]), set([str]) )
    try:
        if restart == False:
            f_visited = open("visited.txt", "r")
            data_v = f_visited.read()
            f_visited.close()
            if data_v == '': raise IOError
            visited = data_v.split(',')
            # visited.pop() #delete last elem-> it s an empty string

            f_deleted = open("deleted_ids.txt", "r")
            data_d = f_deleted.read()
            f_deleted.close()
            if data_d == '': raise IOError
            deleted = data_d.split(',')
            # deleted.pop()  # delete last elem-> it s an empty string

            print 'initializing phase done.'
            print len(visited), 'visited ids'
            return set(visited), set(deleted)
        else:
            f_not_computed = open('not_computed.txt', 'w')
            f_not_computed.close()
            f_deleted = open('deleted_ids.txt', 'w')
            f_deleted.close()
            f_time_log= open('time_log.txt', 'w')
            f_time_log.close()
            return set(), set()
    except IOError :
        #destroy_useless_clique()
        print 'at least one of visited.txt file or deleted_ids.txt not exists or it is empty. computation is starting from scratch'
        f_deleted = open('deleted_ids.txt', 'w')
        f_deleted.close()
        f_not_computed = open('not_computed.txt', 'w')
        f_not_computed.close()
        return set(), set()



def update_visited_file(visited):
    visited_file = open('visited.txt', 'w')
    string_to_write = ",".join(visited)
    visited_file.write(string_to_write)
    visited_file.close()


def create_folders():
    try:
        os.mkdir('log')
    except OSError:
        print 'log folder already exists.'
    try:
        os.mkdir('dumps')
    except OSError:
        print 'dumps folder already exists.'


def rename_files(limit_dataset, iter):
    f1 = 'visited.txt'
    f2= 'not_computed.txt'
    f3 = 'time_log.txt'
    s = str(limit_dataset)+'_iter'+str(iter)+'_'
    os.rename(f1, 'log/'+s+ f1)
    os.rename(f2, 'log/'+s+ f2)
    os.rename(f3, 'log/'+s+ f3)