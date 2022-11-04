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

def plot_histogram(raw, norm_data, max_time):
    xdata, ydata = [],[]
    for key in raw:
        normed = raw[key]/norm_data[key]
        xdata.append(key)
        ydata.append(normed)
        
    plt.scatter(xdata,ydata, marker="|", s=1)
    plt.xlim(0,max_time)
    plt.ylim(0,0.45)
    plt.xlabel("Time remaining (s)")
    plt.ylabel("Probability of blunder")
    plt.title("Blunder % chance with remaining time")
    return xdata, ydata

def plot_existing_data(identifier=None, max_time=300):
    if not identifier:
        identifier = input("Timestamp & params: ")
    rawfile = "data/"+identifier+"_raw.txt"
    normfile = "data/"+identifier+"_norm.txt"
    print("Extracting raw data...")
    raw = get_raw(rawfile)
    print("Extracting normalization dict...")
    with open(normfile, 'r') as infile: 
        content = infile.read(); 
        norm = eval(content);
    print("Plotting...")
    xdata, ydata = plot_histogram(raw,norm,max_time)
    plt.title(identifier)
    print("Done!")
    return     
    
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

def driver(month, min_elo=0, max_elo=5000,min_time=300, max_time=300, savefig = True):
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
    xdata, ydata = plot_histogram(raw,norm, max_time)
    if savefig:
        print("Saving figure...")
        plt.title(str(int(max_time/60))+"min games, ELO: "+str(min_elo)+"-"+str(max_elo))
        plt.savefig(outfile[:-7]+"graph.png", dpi=300)
        plt.clf()
    print("Done!\n")


if __name__ == "__main__":
    start_time = time.perf_counter()
    download_list = open("download.list").readlines()

    for x in range(len(download_list)):
        month =  download_list[x].split("standard/",1)[1]
        bashCommand = "wget --continue -P dataset " + download_list[x]
        process = subprocess.Popen(bashCommand.split(), stdout=subprocess.PIPE)
        output, error = process.communicate()       
        if error:
            print(error)
            break
        
        bashCommand = "pbzip2 -d " + "dataset/" + month
        process = subprocess.Popen(bashCommand.split(), stdout=subprocess.PIPE)
        output, error = process.communicate()       
        if error:
            print(error)
            break

        try:
            p1 = multiprocessing.Process(target=driver, args=(month, 0000, 1000, 180, 180))
            p2 = multiprocessing.Process(target=driver, args=(month, 1000, 2000, 180, 180))
            p3 = multiprocessing.Process(target=driver, args=(month, 2000, 4000, 180, 180))

            p4 = multiprocessing.Process(target=driver, args=(month, 0000,1000, 300, 300))
            p5 = multiprocessing.Process(target=driver, args=(month, 1000,2000, 300, 300))
            p6 = multiprocessing.Process(target=driver, args=(month, 2000,4000, 300, 300))

            p7 = multiprocessing.Process(target=driver, args=(month, 0000, 1000, 600, 600))
            p8 = multiprocessing.Process(target=driver, args=(month, 1000, 2000, 600, 600))
            p9 = multiprocessing.Process(target=driver, args=(month, 2000, 4000, 600, 600))

            p1.start()
            p2.start()
            p3.start()
            p4.start()
            p5.start()
            p6.start()
            p7.start()
            p8.start()
            p9.start()

            finish_time = time.perf_counter()

        except:
            print("stuff broke")

        bashCommand = "rm " + "dataset/" + download_list[x].split("standard/",1)[1]
        process = subprocess.Popen(bashCommand.split(), stdout=subprocess.PIPE)
        output, error = process.communicate()       
        if error:
            print(error)
            break
