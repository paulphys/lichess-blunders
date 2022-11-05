import os
import time
import subprocess
import multiprocessing

def time_to_int(timestring):
    timeint = 0 
    t = timestring.split(":")
    t.reverse()
    mult = 1
    for i in t:
        timeint += mult*int(i)
        mult *= 60
    return timeint

def strip_game(line, time_cutoff_min,time_cutoff_max):
    """
    [[eval, time], [eval, time], [eval,time],...]
    """  
    if time_cutoff_max == None:
        time_cutoff_max = float("Inf")
    game = line.split("}")[:-1] 
    stripped = []
    if game and ("%eval" in game[0]) and ("%clk" in game[0]):
        for line in game:
            res = []
            curr = ""
            read = False
            for char in line:
                if char == "[":
                    read = True
                elif char == "]":
                    res.append(curr.split()[1])
                    curr = ""
                    read = False
                elif read:
                    curr += char
            if len(res) == 2:
                stripped.append(res)
    if stripped:
        if (time_to_int(stripped[0][1]) >= time_cutoff_min) and (time_to_int(stripped[0][1]) <= time_cutoff_max):
            return stripped
    else:
        return False
            

def extract_blunders(stripped,normalization, cutoff = -2, extremis = 100):
    curr_eval = 0.0
    white = True
    blunder_times = []
    all_times = []
    for move in stripped:
        move[1] = time_to_int(move[1])
        all_times.append(move[1])
        prev_eval = curr_eval
        if move[0][0] == "#":
            if move[0][1] == "-":
                curr_eval = -extremis
            else: 
                curr_eval = extremis
        else:
            curr_eval = float(move[0])
        if white:
            white = False
            eval_change = curr_eval - prev_eval
            if eval_change < cutoff and curr_eval < extremis:
                blunder_times.append(move[1])
        else:
            white = True
            eval_change = prev_eval - curr_eval
            if eval_change < cutoff and curr_eval > -extremis:
                blunder_times.append(move[1])
    for t in all_times:
        if t in normalization:
            normalization[t] += 1
        else:
            normalization[t] = 1
    return blunder_times, normalization


def preprocess_PGN(inp, blunder_cutoff, min_elo, max_elo, min_time,max_time):    
    id_string = "time"+str(min_time)+"-"+str(max_time)+"_rating"+str(min_elo)+"-"+str(max_elo)
    start = int(time.time())
    linecount = 0
    validcount = 0
    curr_elo = -1
    normalization = {}
    finished = False
    out = "data/"+str(start)+" "+id_string+"_raw.txt"
    normout = "data/"+str(start)+" "+id_string+"_norm.txt"
    infoout = "data/"+str(start)+" "+id_string+"_info.txt"
    with open(inp,"r") as infile:
        with open(out,"w") as outfile:
            for line in infile:
                if line[:9] == "[WhiteElo":
                    curr_elo = int(line.split()[1][1:-2])
                if (curr_elo < max_elo) and (curr_elo >= min_elo) and (line[:3] == "1. "):
                    stripped = strip_game(line,min_time,max_time)
                    if stripped:
                        validcount +=1
                        blunder_times, normalization = extract_blunders(stripped,normalization)
                        for t in blunder_times:
                            outfile.write(str(t)+"\n")
                linecount += 1
    time_taken = time.time() - start
    print("Finished creating",out," after ",linecount," lines processed. (", validcount," matching games found)")
    print(round(time_taken,3), " seconds taken.")
    with open(normout,"w") as outfile:
        print(normalization, file=outfile)
    with open(infoout,"w") as outfile:
        print("Created",out,"\n",linecount," lines processed. (", validcount," matching games found)",file=outfile)
        print(str(time_taken)+" seconds taken.", file=outfile)
        print("\nParameters used:", file=outfile)
        print("\n Blunder cutoff: ", blunder_cutoff,"\nMin Elo: ", min_elo, "\nMax Elo: ",max_elo, "\nMin time:", min_time, "\nMax time:", max_time, file=outfile)
    return normalization, out

def get_raw(blunderfile="blunders.txt"):
    raw = {}
    with open(blunderfile) as infile:
        for line in infile:
            key = line.replace("\n","")
            if int(key) in raw:
                raw[int(key)] += 1
            else:
                raw[int(key)] = 1
        return raw

def extract(fullpath, min_elo=0, max_elo=5000,min_time=300, max_time=300, savefig = True):
    blunder_cutoff = -2
    norm, outfile = preprocess_PGN(fullpath,blunder_cutoff, min_elo, max_elo, min_time, max_time)
    print("Data processed.")
    total = 0
    for k in norm:
        total += norm[k]
    print("Total blunders: ",total)
    print("Extracting data...")
    raw = get_raw(outfile)
    print("Done extracting")

if __name__ == "__main__":
    total_runtime = time.time()
    download_list = open("download.list").readlines()

    for link in download_list:
        month_runtime = time.time()
        month = link[64:-9]
        fullpath = "raw/" + link[38:]
        print("Downloading... " + month) 
        bashCommand = "wget --continue -P raw/ " + link
        print(bashCommand)
        process = subprocess.Popen(bashCommand.split(), stdout=subprocess.PIPE)
        output, error = process.communicate()       
        if error:
            print(error)
            break
        print("Download done")
        print("Decompressing... " + month)
        bashCommand = "pbzip2 -d " + fullpath
        process = subprocess.Popen(bashCommand.split(), stdout=subprocess.PIPE)
        output, error = process.communicate()       
        if error:
            print(error)
            break
        print("Decompression done")
        fullpath = fullpath[:-4]
        print("Extracting data... " + month)

        try:
            p1 = multiprocessing.Process(target=extract, args=(fullpath, 0000, 1000, 180, 180))
            p2 = multiprocessing.Process(target=extract, args=(fullpath, 1000, 2000, 180, 180))
            p3 = multiprocessing.Process(target=extract, args=(fullpath, 2000, 4000, 180, 180))

            p4 = multiprocessing.Process(target=extract, args=(fullpath, 0000, 1000, 300, 300))
            p5 = multiprocessing.Process(target=extract, args=(fullpath, 1000, 2000, 300, 300))
            p6 = multiprocessing.Process(target=extract, args=(fullpath, 2000, 4000, 300, 300))

            p7 = multiprocessing.Process(target=extract, args=(fullpath, 0000, 1000, 600, 600))
            p8 = multiprocessing.Process(target=extract, args=(fullpath, 1000, 2000, 600, 600))
            p9 = multiprocessing.Process(target=extract, args=(fullpath, 2000, 4000, 600, 600))

            p1.start()
            p2.start()
            p3.start()
            p4.start()
            p5.start()
            p6.start()
            p7.start()
            p8.start()
            p9.start()

        except:
            print("failed")
        
        print("Extraction done")
        print("Removing dataset")
        bashCommand = "rm " + fullpath
        process = subprocess.Popen(bashCommand.split(), stdout=subprocess.PIPE)
        output, error = process.communicate()       
        if error:
            print(error)
            break
        time.sleep(5)
        print("Runtime for ", month, " : ", round( (time.time()-month_runtime)/ 60,2),"min")

    print("Done with all months")
    print("Final runtime: ", round( (time.time()-total_runtime)/ 60,2),"min")
