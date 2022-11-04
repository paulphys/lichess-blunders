import os
import time
import matplotlib.pyplot as plt
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
    This function removes all metadata and move information from game, leaving only something that looks like:
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
    print("Starting...")
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

def driver(month, min_elo=0, max_elo=5000,min_time=300, max_time=300, savefig = False):
    path= "dataset/" + month
    blunder_cutoff = -2
    norm, outfile = preprocess_PGN(path,blunder_cutoff, min_elo, max_elo, min_time, max_time)
    print("Data processed.")
    total = 0
    for k in norm:
        total += norm[k]
    print("Total blunders: ",total)
    print("Extracting data...")
    raw = get_raw(outfile)
    print("Done!\n")


if __name__ == "__main__":
    start_time = time.perf_counter()

    month = "lichess_db_standard_rated_2020-10.pgn"
    try:
        p1 = multiprocessing.Process(target=driver, args=(month, 180, 180, 0000,1000))
        p2 = multiprocessing.Process(target=driver, args=(month, 180, 180, 1000,2000))
        p3 = multiprocessing.Process(target=driver, args=(month, 180, 180, 2000,4000))
        p1.start()
        p2.start()
        p3.start()

        finish_time = time.perf_counter()
        #thread.start_new_thread(driver(month, min_time=180, max_time=180, min_elo=1000, max_elo=2000))
    #  thread.start_new_thread(driver(month, min_time=180, max_time=180, min_elo=2000, max_elo=4000))

                #thread.start_new_thread(driver(month, min_time=300, max_time=300, min_elo=0000, max_elo=1000))
            #  thread.start_new_thread(driver(month, min_time=300, max_time=300, min_elo=1000, max_elo=2000))
            # thread.start_new_thread(driver(month, min_time=300, max_time=300, min_elo=2000, max_elo=4000))
    #
            #   thread.start_new_thread(driver(month, min_time=600, max_time=600, min_elo=0000, max_elo=1000))
            #   thread.start_new_thread(driver(month, min_time=600, max_time=600, min_elo=1000, max_elo=2000))
            #    thread.start_new_thread(driver(month, min_time=600, max_time=600, min_elo=2000, max_elo=4000))

    except:
        print("stuff broke")
        #bashCommand = "rm " + "dataset/" + download_list[x].split("standard/",1)[1]
        #process = subprocess.Popen(bashCommand.split(), stdout=subprocess.PIPE)
        #output, error = process.communicate()       
        #if error:
            #    print(error)
            #   break
