from korg import LineGrokker, PatternRepo
import re
import os

def read_yarnlog(InlogFile,outLogFile):
    fullInFileName=InlogFile
    fullOutFileName=outLogFile
    pr = PatternRepo()  # use the std. logstash grok patterns
    #filename = r"C:\Users\sreddy\OneDrive - MerckGroup\New folder\log_files\1application_1568810042014_190726.log"
    #outcsv = open("1application_1568810042014_190726.csv", "w")
    outcsv = open(fullOutFileName, "w")
    lg = LineGrokker('%{LOGLEVEL:level} %{SPACE}%{JAVALOGMESSAGE:JavaMessage}%{GREEDYDATA:logdata}', pr)
    lg4 = LineGrokker('%{GREEDYDATA:logdata}', pr)
    pline=[]
    cline={}
    with open(fullInFileName, 'r') as filehandle:
        for line in filehandle:
#------------------------------------------------------------
            lineFormat = re.search(r'^(\d+/\d+/\d+)',line)
            if lineFormat != None:
                lg = LineGrokker('%{LOGLEVEL:level} %{SPACE}%{JAVALOGMESSAGE:JavaMessage}%{GREEDYDATA:logdata}', pr)
                cline=lg.grok(line)
            else:
                lg = LineGrokker('%{LOGLEVEL:level}%{JAVALOGMESSAGE:JavaMessage}%{GREEDYDATA:logdata}', pr)
                cline=lg.grok(line)
#------------------------------------------------------------
            #cline=lg.grok(line)
            if (cline is not None) and len(pline)==0:         
                if cline['level']=='ERROR':
                    pline.append(cline)
                #elif cline['level']=='er':
                #    cline=lg4.grok(line)
                #    pline.append(cline)
                else:
                    for i in cline.keys():
                        outcsv.write("%s| " % (cline[i]))
                    outcsv.write("\n")
            elif (cline is not None) and len(pline)!=0:         
                if (cline['level']=='er'):
                    cline=lg4.grok(line)
                    pline.append(cline) 
                elif (cline['level']=='INFO'):
                    for indx in pline:
                        for i in indx.keys():
                            if not re.match(r'\s', indx[i]):
                                if re.match(r'^ERROR', indx[i]):
                                    outcsv.write("%s| " % (indx[i]))
                                else:
                                    outcsv.write("%s " % (indx[i]))                                
                    outcsv.write("\n")
                    for i in cline.keys():
                        outcsv.write("%s| " % (cline[i]))
                    outcsv.write("\n")
                    pline=[]                
    #            else:
    #                print(cline)
    #                for i in cline.keys():
    #                    outcsv.write("%s," % (cline[i]))
    #                outcsv.write("2\n") 
            elif (cline is None) and len(pline)!=0:         
                cline=lg4.grok(line)
                pline.append(cline)
    outcsv.close()
    
#if __name__=="__min__":
#    logInFilePth = r"C:\Users\sreddy\OneDrive - MerckGroup\New folder\log_files"
#    logInFileName = "1application_1568810042014_190726.log"
#    logOutFilePth = r"C:\Users\sreddy\OneDrive - MerckGroup\New folder\log_files"
#    logOutFileName = "1application_1568810042014_190726.csv"
#    
#    fullInLogFileName=os.path.join(logInFilePth,logInFileName)
#    fullOutLogFileName=os.path.join(logOutFilePth,logOutFileName)
#    
#    read_yarnlog(fullInLogFileName,fullOutLogFileName)
