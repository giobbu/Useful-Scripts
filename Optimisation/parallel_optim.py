import os
import shutil
import time
from joblib import Parallel, delayed
import multiprocessing as mp
from multiprocessing import cpu_count


path_optimisation = "path/optimisation/"
path_results = "path/results/"

if not os.path.exists(path_results):
    os.makedirs(path_results)


def optimisation(i, mode):

    batch_past_lst = [200, 200]
    batch_now_lst = [4, 4]
    threshold_lst = [0.1, 0.5]
    minSigma_lst = [30, 30]
    maxSigma_lst = [40, 40]

    batch_past = batch_past_lst[i]
    batch_now = batch_now_lst[i]
    threshold = threshold_lst[i]
    minSigma = minSigma_lst[i]
    maxSigma = maxSigma_lst[i]

    # define file name
    file_name = (
        str(batch_past)
        + "_"
        + str(batch_now)
        + "_"
        + str(threshold)
        + "_"
        + str(minSigma)
        + "_"
        + str(maxSigma)
    )


    # Source path
    srcFolder = path_optimisation + "simulation/"

    # Destination path
    destFolder = path_optimisation + file_name + "_" + mode + "/"

    # Copy the content of
    shutil.copytree(srcFolder, destFolder)

    # change config file
    with open(destFolder + "setting/config.py", 'r') as f:
        fileLines = f.readlines()
        f.close()


    str_root_path = 'root_path = "path/optimisation/data/'
    str_past = "    'past' : "
    str_now = "    'now' : "
    str_tail = "    'batch_files' : \" tail -n "
    str_minSigm = "    'min_sigma_blob': "
    str_maxSigm = "    'max_sigma_blob': "
    str_thresh = "    'threshold_blob': "
    str_blobs_ts = "    'filename_blobs_ts': 'path/optimisation/" + "results/"


    for i in range(len(fileLines)):

        if str_root_path in fileLines[i]:
            fileLines[i] = str_root_path + file_name + '/"\n'
            print(fileLines[i])

        elif str_past in fileLines[i]:
            fileLines[i] = str_past + str(batch_past) + ",\n"
            print(fileLines[i])

        elif str_now in fileLines[i]:
            fileLines[i] = str_now + str(batch_now) + ",\n"
            print(fileLines[i])

        elif str_tail in fileLines[i]:
            fileLines[i] = str_tail + str(batch_past + batch_now) + '",\n'
            print(fileLines[i])
        
        elif str_minSigm in fileLines[i]:
            fileLines[i] = str_minSigm + str(minSigma) + ",\n"
            print(fileLines[i])

        elif str_maxSigm in fileLines[i]:
            fileLines[i] = str_maxSigm + str(maxSigma) + ",\n"
            print(fileLines[i])

        elif str_thresh in fileLines[i]:
            fileLines[i] = str_thresh + str(threshold) + ",\n"
            print(fileLines[i])

        elif str_blobs_ts in fileLines[i]:
            fileLines[i] = str_blobs_ts + file_name + "_" + mode + "_save_blobs_ts.npy',\n"
            print(fileLines[i])

        else:
            pass


    with open(destFolder  + "setting/config.py", "w") as f: 
        f.writelines(fileLines)
        f.close() 


    # run main
    return os.system(
        "python " + destFolder + "main.py > " + path_results + file_name + "_" + mode +   ".log"
    )


def parallel_job(mode):

    assert mode == "joblib" or mode == "multiprocess"

    n_cpu = int(cpu_count()/3)

    if mode == "joblib":
        print("parallel with joblib")
        print("")
        start_time = time.time()
        print("Number of jobs: ", n_cpu)
        Parallel(n_jobs=n_cpu, prefer="processes")(
            delayed(optimisation)(i, mode) for i in range(2)
        )
        end_time = time.time()
        print("processing time ", end_time - start_time)

    else:
        print("parallel with multiprocess")
        print("")
        start_time = time.time()
        print("Number of jobs: ", n_cpu)
        pool = mp.Pool(n_cpu)
        for i in range(2): result = pool.apply_async(optimisation, args=(i, mode))
        pool.close()  # end pool
        pool.join()  # wait for all tasks to finish
        end_time = time.time()
        print("processing time ", end_time - start_time)
        print("")


# Parallel computing Joblib
if __name__ == "__main__":

    parallel_job("multiprocess")

    #parallel_job("joblib")
