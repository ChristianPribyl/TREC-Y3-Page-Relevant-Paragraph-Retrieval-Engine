import os
import time
from datetime import datetime
import sys

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

tfidf_variants2 = [
    "ltn.nnn",
    "lnn.nnn"
]

doubleVariants = [
    0.0, 0.2, 0.4, 0.6, 0.8, 1.0
]

doubleVariants2 = [
    0.0, 0.5, 1.0
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

def runTfidfModels2(resultsDir):
    os.system("rm *.run; rm *.timeInSeconds")
    for filterOption in ["filter", "nofilter"]:
        for index in [smallIndex, bigIndex]:
            for mergeType in facetMergeVariations:
                for tfidfVariant in tfidf_variants2:
                    outfile = f"tfidf-{tfidfVariant}-{index.replace('.db', '').replace('.', '').replace('/', '')}-{filterOption}-{mergeType}"
                    print(f"{str(datetime.now())} {outfile}")
                    start = time.time()
                    os.system(f"java -jar target/{jar} tfidf-cbor-query {index} {cborOutlines} {qrel} {tfidfVariant} {filterOption} {mergeType} {outfile + '.run'}")
                    end = time.time()
                    with open(outfile + ".timeInSeconds", 'w') as f:
                        f.write("%.2f"%(end-start))
    os.system(f"mv *.run {resultsDir}; mv *.timeInSeconds {resultsDir}")


def runBm25Models(resultsDir, index): #only 1 index (1/2 runs)
    os.system("rm *.run; rm *.timeInSeconds")
    for filterOption in ["filter"]: #always filter (1/2 runs)
        for mergeType in facetMergeVariations:
            for k1 in doubleVariants:
                for k3 in doubleVariants2:
                    for beta in doubleVariants2:
                        outfile = f"bm25-{k1}-{k3}-{beta}-{index.replace('.db', '').replace('.', '').replace('/', '')}-{filterOption}-{mergeType}"
                        print(f"{str(datetime.now())} {outfile}")
                        start = time.time()
                        os.system(f"java -jar target/{jar} bm25-cbor-query {index} {cborOutlines} {qrel} {k1} {k3} {beta} {filterOption} {mergeType} {outfile + '.run'}")
                        end = time.time()
                        with open(outfile + ".timeInSeconds", 'w') as f:
                            f.write("%.2f"%(end-start))
            for k1 in doubleVariants2:
                for k3 in doubleVariants:
                    for beta in doubleVariants2:
                        outfile = f"bm25-{k1}-{k3}-{beta}-{index.replace('.db', '').replace('.', '').replace('/', '')}-{filterOption}-{mergeType}"
                        print(f"{str(datetime.now())} {outfile}")
                        start = time.time()
                        os.system(f"java -jar target/{jar} bm25-cbor-query {index} {cborOutlines} {qrel} {k1} {k3} {beta} {filterOption} {mergeType} {outfile + '.run'}")
                        end = time.time()
                        with open(outfile + ".timeInSeconds", 'w') as f:
                            f.write("%.2f"%(end-start))
            for k1 in doubleVariants2:
                for k3 in doubleVariants2:
                    for beta in doubleVariants:
                        outfile = f"bm25-{k1}-{k3}-{beta}-{index.replace('.db', '').replace('.', '').replace('/', '')}-{filterOption}-{mergeType}"
                        print(f"{str(datetime.now())} {outfile}")
                        start = time.time()
                        os.system(f"java -jar target/{jar} bm25-cbor-query {index} {cborOutlines} {qrel} {k1} {k3} {beta} {filterOption} {mergeType} {outfile + '.run'}")
                        end = time.time()
                        with open(outfile + ".timeInSeconds", 'w') as f:
                            f.write("%.2f"%(end-start))

def runBimModels(resultsDir):
    os.system("rm *.run; rm *.timeInSeconds")
    for filterOption in ["filter", "nofilter"]:
        for index in [smallIndex, bigIndex]:
            for mergeType in facetMergeVariations:
                outfile = f"bim-{index.replace('.db', '').replace('.', '').replace('/', '')}-{filterOption}-{mergeType}"
                print(f"{str(datetime.now())} {outfile}")
                start = time.time()
                os.system(f"java -jar target/{jar} bim {index} {cborOutlines} {qrel} {filterOption} {mergeType} {outfile + '.run'}")
                end = time.time()
                with open(outfile + ".timeInSeconds", 'w') as f:
                    f.write("%.2f"%(end-start))

def runJelinekMercer(resultsDir, index):
    os.system("rm *.run; rm *.timeInSeconds")
    for filterOption in ["filter"]:
        for index in [smallIndex, bigIndex]:
            for mergeType in facetMergeVariations:
                for beta in doubleVariants:
                    outfile = f"jelinek-mercer-{beta}-{index.replace('.db', '').replace('.', '').replace('/', '')}-{filterOption}-{mergeType}"
                    print(f"{str(datetime.now())} {outfile}")
                    start = time.time()
                    os.system(f"java -jar target/{jar} jelinekMercerCborQuery {index} {cborOutlines} {qrel} {beta} {filterOption} {mergeType} {outfile + '.run'}")
                    end = time.time()
                    with open(outfile + ".timeInSeconds", 'w') as f:
                        f.write("%.2f"%(end-start))


def runTest():
    os.system(f"java -jar target/{jar} tfidf-cbor-query {smallIndex} {cborOutlines} {qrel} btn.bnn filter rankRecurrance {'test.run'}")

def recurranceTfidf():
    os.system("rm *.run; rm *.timeInSeconds")
    for filterOption in ["filter", "nofilter"]:
        for index in [smallIndex, bigIndex]:
            for mergeType in ['recurrance']:
                for tfidfVariant in tfidf_variants:
                    outfile = f"tfidf-{tfidfVariant}-{index.replace('.db', '').replace('.', '').replace('/', '')}-{filterOption}-{mergeType}"
                    print(f"{str(datetime.now())} {outfile}")
                    start = time.time()
                    os.system(f"java -jar target/{jar} tfidf-cbor-query {index} {cborOutlines} {qrel} {tfidfVariant} {filterOption} {mergeType} {outfile + '.run'}")
                    end = time.time()
                    with open(outfile + ".timeInSeconds", 'w') as f:
                        f.write("%.2f"%(end-start))
    for filterOption in ["filter", "nofilter"]:
        for index in [smallIndex, bigIndex]:
            for mergeType in ['recurrance']:
                for tfidfVariant in tfidf_variants2:
                    outfile = f"tfidf-{tfidfVariant}-{index.replace('.db', '').replace('.', '').replace('/', '')}-{filterOption}-{mergeType}"
                    print(f"{str(datetime.now())} {outfile}")
                    start = time.time()
                    os.system(f"java -jar target/{jar} tfidf-cbor-query {index} {cborOutlines} {qrel} {tfidfVariant} {filterOption} {mergeType} {outfile + '.run'}")
                    end = time.time()
                    with open(outfile + ".timeInSeconds", 'w') as f:
                        f.write("%.2f"%(end-start))
    os.system(f"mv *.run {resultsDir}; mv *.timeInSeconds {resultsDir}")

def recurranceBim():
    os.system("rm *.run; rm *.timeInSeconds")
    for filterOption in ["filter", "nofilter"]:
        for index in [smallIndex, bigIndex]:
            for mergeType in ['recurrance']:
                outfile = f"bim-{index.replace('.db', '').replace('.', '').replace('/', '')}-{filterOption}-{mergeType}"
                print(f"{str(datetime.now())} {outfile}")
                start = time.time()
                os.system(f"java -jar target/{jar} bim {index} {cborOutlines} {qrel} {filterOption} {mergeType} {outfile + '.run'}")
                end = time.time()
                with open(outfile + ".timeInSeconds", 'w') as f:
                    f.write("%.2f"%(end-start))

def recurranceJelinek():
    os.system("rm *.run; rm *.timeInSeconds")
    for filterOption in ["filter"]:
        for index in [smallIndex, bigIndex]:
            for mergeType in ['rankRecurrance', 'roundRobin']:
                for beta in [0.8, 1.0]:
                    outfile = f"jelinek-mercer-{beta}-{index.replace('.db', '').replace('.', '').replace('/', '')}-{filterOption}-{mergeType}"
                    print(f"{str(datetime.now())} {outfile}")
                    start = time.time()
                    os.system(f"java -jar target/{jar} jelinekMercerCborQuery {index} {cborOutlines} {qrel} {beta} {filterOption} {mergeType} {outfile + '.run'}")
                    end = time.time()
                    with open(outfile + ".timeInSeconds", 'w') as f:
                        f.write("%.2f"%(end-start))
    for filterOption in ["nofilter"]:
        for index in [smallIndex, bigIndex]:
            for mergeType in mergeTypes:
                for beta in doubleVariants:
                    outfile = f"jelinek-mercer-{beta}-{index.replace('.db', '').replace('.', '').replace('/', '')}-{filterOption}-{mergeType}"
                    print(f"{str(datetime.now())} {outfile}")
                    start = time.time()
                    os.system(f"java -jar target/{jar} jelinekMercerCborQuery {index} {cborOutlines} {qrel} {beta} {filterOption} {mergeType} {outfile + '.run'}")
                    end = time.time()
                    with open(outfile + ".timeInSeconds", 'w') as f:
                        f.write("%.2f"%(end-start))

def bm25small():
    index = smallIndex
    os.system("rm *.run; rm *.timeInSeconds")
    for filterOption in ['filter', 'nofilter']:
        for mergeType in facetMergeVariations:
            for n in [[0.0, 0.0, 0.0], [0.0, 0.0, 1.0], [0.0, 1.0, 0.0], [1.0, 0.0, 0.0],
                    [1.0, 0.5, 0.0], [1.0, 1.0, 0.0], [0.6, 1.0, 0.0], [0.2, 1.0, 0.0],
                    [1.0, 1.0, 1.0], [1.0, 1.0, 0.5]]:
                k1 = n[0]
                k3 = n[1]
                beta = n[2]
                outfile = f"bm25-{k1}-{k3}-{beta}-{index.replace('.db', '').replace('.', '').replace('/', '')}-{filterOption}-{mergeType}"
                print(f"{str(datetime.now())} {outfile}")
                start = time.time()
                os.system(f"java -jar target/{jar} bm25-cbor-query {index} {cborOutlines} {qrel} {k1} {k3} {beta} {filterOption} {mergeType} {outfile + '.run'}")
                end = time.time()
                with open(outfile + ".timeInSeconds", 'w') as f:
                    f.write("%.2f"%(end-start))


model = sys.argv[1]
os.system('mkdir -p ../results')
if model == 'tfidf':
    runTfidfModels("../results")
elif model == 'bm25':
    runBm25Models("../results", smallIndex)
    runBm25Models("../results", bigIndex)
elif model == 'bim':
    runBimModels("../results")
elif model == 'jelinek-mercer':
    runJelinekMercer('../results', smallIndex)
    runJelinekMercer('../results', bigIndex)
elif model == 'tfidf2':
    runTfidfModels2("../results")
elif model == 'test':
    runTest()
elif model == 'tfidf3':
    recurranceTfidf()
elif model == 'bim2':
    recurranceBim()
elif model == 'jelinek2':
    recurranceJelinek()
elif model == 'bm25small':
    bm25small()