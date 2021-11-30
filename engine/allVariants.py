import os
import time
from datetime import datetime

bigIndex = "./indexFull.db"
smallIndex = "./indexOnlyScored.db"
cborOutlines = "../benchmarkY3test.cbor-outlines.cbor"
qrel = "../trec_pages.qrel"
jar = "ir-engine-0.jar"

tfidf_variants = [
    "nnn.nnn",
    "apn.anc",
    "atc.anc",
    "atn.ann",
    "btn.bnn",
    "npn.nnn",
    "ntc.nnc",
    "ntn.nnn"
]

doubleVariants = [
    0.0, 0.2, 0.4, 0.6, 0.8, 1.0
]

facetMergeVariations = [
    "roundRobin",
    "recurrance",
    "rankRecurrance"
]

# filter | nofilter
# indexFull | indexInParts
# roundrobin | recurrance | rankrecurrance
def runTfidfModels(resultsDir):
    os.system("rm *.run; rm *.timeInSeconds")
    for filterOption in ["filter", "nofilter"]:
        for index in [smallIndex, bigIndex]:
            for mergeType in facetMergeVariations:
                for tfidfVariant in tfidf_variants:
                    outfile = f"tfidf-{tfidfVariant}-{index.replace('.db', '').replace('.', '').replace('/', '')}-{filterOption}-{mergeType}"
                    print(f"{str(datetime.now())} {outfile}")
                    start = time.time()
                    os.system(f"java -jar target/{jar} tfidf-cbor-query {index} {cborOutlines} {qrel} {tfidfVariant} {filterOption} {mergeType} {outfile + '.run'}")
                    end = time.time()
                    with open(outfile + ".timeInSeconds", 'w') as f:
                        f.write("%.2f"%(end-start))
    os.system(f"mv *.run {resultsDir}; mv *.timeInSeconds {resultsDir}")

os.system("rm -rf ../results")
os.system("mkdir -p ../results")
runTfidfModels("../results")