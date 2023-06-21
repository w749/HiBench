/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.intel.sparkbench.micro

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.IOUtils
import org.apache.spark.{SparkConf, SparkContext}

import java.util.Base64
import scala.collection.mutable
import scala.language.postfixOps
import scala.util.Random

/*
 * 准备Join数据。共输出两份数据后续用来join，其中一份包含倾斜数据和其它数据，第二份是用来join的数据，包含倾斜的key和其它的key
 */
object ScalaJoinPrepare {
    def main(args: Array[String]){
        if (args.length < 4){
            System.err.println(
                s"Usage: $ScalaJoinPrepare <OUTPUT_HDFS> <PARTITION> <DATA_SIZE> <THRESHOLD_SKEW>"
            )
            System.exit(1)
        }
        val output = args(0)
        val partition = args(1).toInt
        val dataSize = args(2).toLong
        val thresholdSkew = args(3).toDouble
        val batchSize = 1000000L
        val separate = ","
        val skewOutput = output + "/skew"
        val joinOutput = output + "/join"
        val joinDataSize = 100

        val sparkConf = new SparkConf().setAppName("ScalaJoinPrepare")
        val sc = new SparkContext(sparkConf)

        var partitionSizeArray = Array.empty[Long]
        var allSizeArray = Array.empty[(Int, Long)]
        val eachPartitionSize = dataSize / partition
        val remainSize = dataSize - (partition * eachPartitionSize)
        if (remainSize == 0) {
            partitionSizeArray = Array.fill(partition)(eachPartitionSize)
            allSizeArray = partitionSizeArray.zipWithIndex.map(kv => (kv._2, kv._1))
        } else {
            partitionSizeArray = Array.fill(partition - 1)(eachPartitionSize)
            allSizeArray = (partitionSizeArray ++ Array(remainSize)).zipWithIndex.map(kv => (kv._2, kv._1))
        }

        sc.parallelize(allSizeArray, partition)
          .keyBy(_._1)
          .foreach(line => {
              var fs: FileSystem = null
              val configuration = new Configuration()
              // 禁用FieSystem缓存
              configuration.set("fs.hdfs.impl.disable.cache", "true")
              this.synchronized {
                  fs = FileSystem.get(configuration)
              }
              val partitionIndex = line._1
              val allSize = line._2._2
              val dirPath = new Path(skewOutput)
              val filePath = new Path(s"${skewOutput}/${partitionIndex}")
              val tmpFilePath = new Path(s"${skewOutput}/${partitionIndex}.tmp")
              if (!fs.exists(dirPath)) fs.mkdirs(dirPath)

              val skewSize = (allSize * thresholdSkew).toLong
              val otherSize = allSize - skewSize

              generateData(skewSize, batchSize, fs, filePath, tmpFilePath, isSkew = true)
              generateData(otherSize, batchSize, fs, filePath, tmpFilePath)
          })

        val joinData = Array.fill(joinDataSize - 1) {
            val tmpKey = WORDS_ARRAY(Random.nextInt(WORDS_ARRAY.length - 1))
            val key = Base64.getEncoder.encodeToString(tmpKey.getBytes)
            val value = WORDS_ARRAY(Random.nextInt(WORDS_ARRAY.length - 1))
            key + separate + value
        }
        val joinSkewKV = Base64.getEncoder.encodeToString(SKEW_WORD.head.getBytes) + separate + SKEW_WORD.head
        sc.parallelize(joinData ++ Array(joinSkewKV), 1)
          .saveAsTextFile(joinOutput)

        sc.stop()
    }

    def generateData(allSize: Long, batchSize: Long, fs: FileSystem, filePath: Path, tmpFilePath: Path, isSkew: Boolean = false, separate: String = ","): Unit = {
        val wordsArray = WORDS_ARRAY
        val skewWord = SKEW_WORD.head
        val batchCount = allSize / batchSize
        val remainSize = allSize - (batchCount * batchSize)
        for (_ <- 0 until batchCount.toInt) {
            val array = mutable.ArrayBuffer[String]()
            var tmpBatchSize = batchSize
            while (tmpBatchSize > 0) {
                val tmpKey = if (isSkew) skewWord else wordsArray(Random.nextInt(wordsArray.length - 1))
                val key = Base64.getEncoder.encodeToString(tmpKey.getBytes)
                val valueBuffer = new StringBuffer()
                (10 to 10 + Random.nextInt(10)).foreach(_ => {
                    valueBuffer.append(wordsArray(Random.nextInt(wordsArray.length - 1))).append(" ")
                })
                array.append(key + separate + valueBuffer.toString)
                tmpBatchSize -= 1
            }
            writeArray(array, fs, filePath, tmpFilePath)
        }

        val array = mutable.ArrayBuffer[String]()
        var tmpBatchSize = remainSize
        while (tmpBatchSize > 0) {
            val tmpKey = if (isSkew) skewWord else wordsArray(Random.nextInt(wordsArray.length - 1))
            val key = Base64.getEncoder.encodeToString(tmpKey.getBytes)
            val valueBuffer = new StringBuffer()
            (10 to 10 + Random.nextInt(10)).foreach(_ => {
                valueBuffer.append(wordsArray(Random.nextInt(wordsArray.length - 1))).append(" ")
            })
            array.append(key + separate + valueBuffer.toString)
            tmpBatchSize -= 1
        }
        writeArray(array, fs, filePath, tmpFilePath)
    }

    def writeArray(array: mutable.ArrayBuffer[String], fs: FileSystem, filePath: Path, tmpFilePath: Path): Unit = {
        if (fs.exists(tmpFilePath)) fs.delete(tmpFilePath, false)
        if (fs.exists(filePath)) {
            val inputStream = fs.open(filePath)
            val outputStream = fs.create(tmpFilePath)
            IOUtils.copyBytes(inputStream, outputStream, 1024)
            array.foreach(line => outputStream.write(s"${line}\n".getBytes("UTF-8")))
            outputStream.flush()
            inputStream.close()
            outputStream.close()
            fs.delete(filePath, false)
            fs.rename(tmpFilePath, filePath)
        } else {
            val stream = fs.create(filePath)
            array.foreach(line => stream.write(s"${line}\n".getBytes("UTF-8")))
            stream.flush()
            stream.close()
        }
        array.clear()
    }

    val SKEW_WORD = Array("hello")
    val WORDS_ARRAY = Array(
        "diurnalness", "Homoiousian",
        "spiranthic", "tetragynian",
        "silverhead", "ungreat",
        "lithograph", "exploiter",
        "physiologian", "by",
        "hellbender", "Filipendula",
        "undeterring", "antiscolic",
        "pentagamist", "hypoid",
        "cacuminal", "sertularian",
        "schoolmasterism", "nonuple",
        "gallybeggar", "phytonic",
        "swearingly", "nebular",
        "Confervales", "thermochemically",
        "characinoid", "cocksuredom",
        "fallacious", "feasibleness",
        "debromination", "playfellowship",
        "tramplike", "testa",
        "participatingly", "unaccessible",
        "bromate", "experientialist",
        "roughcast", "docimastical",
        "choralcelo", "blightbird",
        "peptonate", "sombreroed",
        "unschematized", "antiabolitionist",
        "besagne", "mastication",
        "bromic", "sviatonosite",
        "cattimandoo", "metaphrastical",
        "endotheliomyoma", "hysterolysis",
        "unfulminated", "Hester",
        "oblongly", "blurredness",
        "authorling", "chasmy",
        "Scorpaenidae", "toxihaemia",
        "Dictograph", "Quakerishly",
        "deaf", "timbermonger",
        "strammel", "Thraupidae",
        "seditious", "plerome",
        "Arneb", "eristically",
        "serpentinic", "glaumrie",
        "socioromantic", "apocalypst",
        "tartrous", "Bassaris",
        "angiolymphoma", "horsefly",
        "kenno", "astronomize",
        "euphemious", "arsenide",
        "untongued", "parabolicness",
        "uvanite", "helpless",
        "gemmeous", "stormy",
        "templar", "erythrodextrin",
        "comism", "interfraternal",
        "preparative", "parastas",
        "frontoorbital", "Ophiosaurus",
        "diopside", "serosanguineous",
        "ununiformly", "karyological",
        "collegian", "allotropic",
        "depravity", "amylogenesis",
        "reformatory", "epidymides",
        "pleurotropous", "trillium",
        "dastardliness", "coadvice",
        "embryotic", "benthonic",
        "pomiferous", "figureheadship",
        "Megaluridae", "Harpa",
        "frenal", "commotion",
        "abthainry", "cobeliever",
        "manilla", "spiciferous",
        "nativeness", "obispo",
        "monilioid", "biopsic",
        "valvula", "enterostomy",
        "planosubulate", "pterostigma",
        "lifter", "triradiated",
        "venialness", "tum",
        "archistome", "tautness",
        "unswanlike", "antivenin",
        "Lentibulariaceae", "Triphora",
        "angiopathy", "anta",
        "Dawsonia", "becomma",
        "Yannigan", "winterproof",
        "antalgol", "harr",
        "underogating", "ineunt",
        "cornberry", "flippantness",
        "scyphostoma", "approbation",
        "Ghent", "Macraucheniidae",
        "scabbiness", "unanatomized",
        "photoelasticity", "eurythermal",
        "enation", "prepavement",
        "flushgate", "subsequentially",
        "Edo", "antihero",
        "Isokontae", "unforkedness",
        "porriginous", "daytime",
        "nonexecutive", "trisilicic",
        "morphiomania", "paranephros",
        "botchedly", "impugnation",
        "Dodecatheon", "obolus",
        "unburnt", "provedore",
        "Aktistetae", "superindifference",
        "Alethea", "Joachimite",
        "cyanophilous", "chorograph",
        "brooky", "figured",
        "periclitation", "quintette",
        "hondo", "ornithodelphous",
        "unefficient", "pondside",
        "bogydom", "laurinoxylon",
        "Shiah", "unharmed",
        "cartful", "noncrystallized",
        "abusiveness", "cromlech",
        "japanned", "rizzomed",
        "underskin", "adscendent",
        "allectory", "gelatinousness",
        "volcano", "uncompromisingly",
        "cubit", "idiotize",
        "unfurbelowed", "undinted",
        "magnetooptics", "Savitar",
        "diwata", "ramosopalmate",
        "Pishquow", "tomorn",
        "apopenptic", "Haversian",
        "Hysterocarpus", "ten",
        "outhue", "Bertat",
        "mechanist", "asparaginic",
        "velaric", "tonsure",
        "bubble", "Pyrales",
        "regardful", "glyphography",
        "calabazilla", "shellworker",
        "stradametrical", "havoc",
        "theologicopolitical", "sawdust",
        "diatomaceous", "jajman",
        "temporomastoid", "Serrifera",
        "Ochnaceae", "aspersor",
        "trailmaking", "Bishareen",
        "digitule", "octogynous",
        "epididymitis", "smokefarthings",
        "bacillite", "overcrown",
        "mangonism", "sirrah",
        "undecorated", "psychofugal",
        "bismuthiferous", "rechar",
        "Lemuridae", "frameable",
        "thiodiazole", "Scanic",
        "sportswomanship", "interruptedness",
        "admissory", "osteopaedion",
        "tingly", "tomorrowness",
        "ethnocracy", "trabecular",
        "vitally", "fossilism",
        "adz", "metopon",
        "prefatorial", "expiscate",
        "diathermacy", "chronist",
        "nigh", "generalizable",
        "hysterogen", "aurothiosulphuric",
        "whitlowwort", "downthrust",
        "Protestantize", "monander",
        "Itea", "chronographic",
        "silicize", "Dunlop",
        "eer", "componental",
        "spot", "pamphlet",
        "antineuritic", "paradisean",
        "interruptor", "debellator",
        "overcultured", "Florissant",
        "hyocholic", "pneumatotherapy",
        "tailoress", "rave",
        "unpeople", "Sebastian",
        "thermanesthesia", "Coniferae",
        "swacking", "posterishness",
        "ethmopalatal", "whittle",
        "analgize", "scabbardless",
        "naught", "symbiogenetically",
        "trip", "parodist",
        "columniform", "trunnel",
        "yawler", "goodwill",
        "pseudohalogen", "swangy",
        "cervisial", "mediateness",
        "genii", "imprescribable",
        "pony", "consumptional",
        "carposporangial", "poleax",
        "bestill", "subfebrile",
        "sapphiric", "arrowworm",
        "qualminess", "ultraobscure",
        "thorite", "Fouquieria",
        "Bermudian", "prescriber",
        "elemicin", "warlike",
        "semiangle", "rotular",
        "misthread", "returnability",
        "seraphism", "precostal",
        "quarried", "Babylonism",
        "sangaree", "seelful",
        "placatory", "pachydermous",
        "bozal", "galbulus",
        "spermaphyte", "cumbrousness",
        "pope", "signifier",
        "Endomycetaceae", "shallowish",
        "sequacity", "periarthritis",
        "bathysphere", "pentosuria",
        "Dadaism", "spookdom",
        "Consolamentum", "afterpressure",
        "mutter", "louse",
        "ovoviviparous", "corbel",
        "metastoma", "biventer",
        "Hydrangea", "hogmace",
        "seizing", "nonsuppressed",
        "oratorize", "uncarefully",
        "benzothiofuran", "penult",
        "balanocele", "macropterous",
        "dishpan", "marten",
        "absvolt", "jirble",
        "parmelioid", "airfreighter",
        "acocotl", "archesporial",
        "hypoplastral", "preoral",
        "quailberry", "cinque",
        "terrestrially", "stroking",
        "limpet", "moodishness",
        "canicule", "archididascalian",
        "pompiloid", "overstaid",
        "introducer", "Italical",
        "Christianopaganism", "prescriptible",
        "subofficer", "danseuse",
        "cloy", "saguran",
        "frictionlessly", "deindividualization",
        "Bulanda", "ventricous",
        "subfoliar", "basto",
        "scapuloradial", "suspend",
        "stiffish", "Sphenodontidae",
        "eternal", "verbid",
        "mammonish", "upcushion",
        "barkometer", "concretion",
        "preagitate", "incomprehensible",
        "tristich", "visceral",
        "hemimelus", "patroller",
        "stentorophonic", "pinulus",
        "kerykeion", "brutism",
        "monstership", "merciful",
        "overinstruct", "defensibly",
        "bettermost", "splenauxe",
        "Mormyrus", "unreprimanded",
        "taver", "ell",
        "proacquittal", "infestation",
        "overwoven", "Lincolnlike",
        "chacona", "Tamil",
        "classificational", "lebensraum",
        "reeveland", "intuition",
        "Whilkut", "focaloid",
        "Eleusinian", "micromembrane",
        "byroad", "nonrepetition",
        "bacterioblast", "brag",
        "ribaldrous", "phytoma",
        "counteralliance", "pelvimetry",
        "pelf", "relaster",
        "thermoresistant", "aneurism",
        "molossic", "euphonym",
        "upswell", "ladhood",
        "phallaceous", "inertly",
        "gunshop", "stereotypography",
        "laryngic", "refasten",
        "twinling", "oflete",
        "hepatorrhaphy", "electrotechnics",
        "cockal", "guitarist",
        "topsail", "Cimmerianism",
        "larklike", "Llandovery",
        "pyrocatechol", "immatchable",
        "chooser", "metrocratic",
        "craglike", "quadrennial",
        "nonpoisonous", "undercolored",
        "knob", "ultratense",
        "balladmonger", "slait",
        "sialadenitis", "bucketer",
        "magnificently", "unstipulated",
        "unscourged", "unsupercilious",
        "packsack", "pansophism",
        "soorkee", "percent",
        "subirrigate", "champer",
        "metapolitics", "spherulitic",
        "involatile", "metaphonical",
        "stachyuraceous", "speckedness",
        "bespin", "proboscidiform",
        "gul", "squit",
        "yeelaman", "peristeropode",
        "opacousness", "shibuichi",
        "retinize", "yote",
        "misexposition", "devilwise",
        "pumpkinification", "vinny",
        "bonze", "glossing",
        "decardinalize", "transcortical",
        "serphoid", "deepmost",
        "guanajuatite", "wemless",
        "arval", "lammy",
        "Effie", "Saponaria",
        "tetrahedral", "prolificy",
        "excerpt", "dunkadoo",
        "Spencerism", "insatiately",
        "Gilaki", "oratorship",
        "arduousness", "unbashfulness",
        "Pithecolobium", "unisexuality",
        "veterinarian", "detractive",
        "liquidity", "acidophile",
        "proauction", "sural",
        "totaquina", "Vichyite",
        "uninhabitedness", "allegedly",
        "Gothish", "manny",
        "Inger", "flutist",
        "ticktick", "Ludgatian",
        "homotransplant", "orthopedical",
        "diminutively", "monogoneutic",
        "Kenipsim", "sarcologist",
        "drome", "stronghearted",
        "Fameuse", "Swaziland",
        "alen", "chilblain",
        "beatable", "agglomeratic",
        "constitutor", "tendomucoid",
        "porencephalous", "arteriasis",
        "boser", "tantivy",
        "rede", "lineamental",
        "uncontradictableness", "homeotypical",
        "masa", "folious",
        "dosseret", "neurodegenerative",
        "subtransverse", "Chiasmodontidae",
        "palaeotheriodont", "unstressedly",
        "chalcites", "piquantness",
        "lampyrine", "Aplacentalia",
        "projecting", "elastivity",
        "isopelletierin", "bladderwort",
        "strander", "almud",
        "iniquitously", "theologal",
        "bugre", "chargeably",
        "imperceptivity", "meriquinoidal",
        "mesophyte", "divinator",
        "perfunctory", "counterappellant",
        "synovial", "charioteer",
        "crystallographical", "comprovincial",
        "infrastapedial", "pleasurehood",
        "inventurous", "ultrasystematic",
        "subangulated", "supraoesophageal",
        "Vaishnavism", "transude",
        "chrysochrous", "ungrave",
        "reconciliable", "uninterpleaded",
        "erlking", "wherefrom",
        "aprosopia", "antiadiaphorist",
        "metoxazine", "incalculable",
        "umbellic", "predebit",
        "foursquare", "unimmortal",
        "nonmanufacture", "slangy",
        "predisputant", "familist",
        "preaffiliate", "friarhood",
        "corelysis", "zoonitic",
        "halloo", "paunchy",
        "neuromimesis", "aconitine",
        "hackneyed", "unfeeble",
        "cubby", "autoschediastical",
        "naprapath", "lyrebird",
        "inexistency", "leucophoenicite",
        "ferrogoslarite", "reperuse",
        "uncombable", "tambo",
        "propodiale", "diplomatize",
        "Russifier", "clanned",
        "corona", "michigan",
        "nonutilitarian", "transcorporeal",
        "bought", "Cercosporella",
        "stapedius", "glandularly",
        "pictorially", "weism",
        "disilane", "rainproof",
        "Caphtor", "scrubbed",
        "oinomancy", "pseudoxanthine",
        "nonlustrous", "redesertion",
        "Oryzorictinae", "gala",
        "Mycogone", "reappreciate",
        "cyanoguanidine", "seeingness",
        "breadwinner", "noreast",
        "furacious", "epauliere",
        "omniscribent", "Passiflorales",
        "uninductive", "inductivity",
        "Orbitolina", "Semecarpus",
        "migrainoid", "steprelationship",
        "phlogisticate", "mesymnion",
        "sloped", "edificator",
        "beneficent", "culm",
        "paleornithology", "unurban",
        "throbless", "amplexifoliate",
        "sesquiquintile", "sapience",
        "astucious", "dithery",
        "boor", "ambitus",
        "scotching", "uloid",
        "uncompromisingness", "hoove",
        "waird", "marshiness",
        "Jerusalem", "mericarp",
        "unevoked", "benzoperoxide",
        "outguess", "pyxie",
        "hymnic", "euphemize",
        "mendacity", "erythremia",
        "rosaniline", "unchatteled",
        "lienteria", "Bushongo",
        "dialoguer", "unrepealably",
        "rivethead", "antideflation",
        "vinegarish", "manganosiderite",
        "doubtingness", "ovopyriform",
        "Cephalodiscus", "Muscicapa",
        "Animalivora", "angina",
        "planispheric", "ipomoein",
        "cuproiodargyrite", "sandbox",
        "scrat", "Munnopsidae",
        "shola", "pentafid",
        "overstudiousness", "times",
        "nonprofession", "appetible",
        "valvulotomy", "goladar",
        "uniarticular", "oxyterpene",
        "unlapsing", "omega",
        "trophonema", "seminonflammable",
        "circumzenithal", "starer",
        "depthwise", "liberatress",
        "unleavened", "unrevolting",
        "groundneedle", "topline",
        "wandoo", "umangite",
        "ordinant", "unachievable",
        "oversand", "snare",
        "avengeful", "unexplicit",
        "mustafina", "sonable",
        "rehabilitative", "eulogization",
        "papery", "technopsychology",
        "impressor", "cresylite",
        "entame", "transudatory",
        "scotale", "pachydermatoid",
        "imaginary", "yeat",
        "slipped", "stewardship",
        "adatom", "cockstone",
        "skyshine", "heavenful",
        "comparability", "exprobratory",
        "dermorhynchous", "parquet",
        "cretaceous", "vesperal",
        "raphis", "undangered",
        "Glecoma", "engrain",
        "counteractively", "Zuludom",
        "orchiocatabasis", "Auriculariales",
        "warriorwise", "extraorganismal",
        "overbuilt", "alveolite",
        "tetchy", "terrificness",
        "widdle", "unpremonished",
        "rebilling", "sequestrum",
        "equiconvex", "heliocentricism",
        "catabaptist", "okonite",
        "propheticism", "helminthagogic",
        "calycular", "giantly",
        "wingable", "golem",
        "unprovided", "commandingness",
        "greave", "haply",
        "doina", "depressingly",
        "subdentate", "impairment",
        "decidable", "neurotrophic",
        "unpredict", "bicorporeal",
        "pendulant", "flatman",
        "intrabred", "toplike",
        "Prosobranchiata", "farrantly",
        "toxoplasmosis", "gorilloid",
        "dipsomaniacal", "aquiline",
        "atlantite", "ascitic",
        "perculsive", "prospectiveness",
        "saponaceous", "centrifugalization",
        "dinical", "infravaginal",
        "beadroll", "affaite",
        "Helvidian", "tickleproof",
        "abstractionism", "enhedge",
        "outwealth", "overcontribute",
        "coldfinch", "gymnastic",
        "Pincian", "Munychian",
        "codisjunct", "quad",
        "coracomandibular", "phoenicochroite",
        "amender", "selectivity",
        "putative", "semantician",
        "lophotrichic", "Spatangoidea",
        "saccharogenic", "inferent",
        "Triconodonta", "arrendation",
        "sheepskin", "taurocolla",
        "bunghole", "Machiavel",
        "triakistetrahedral", "dehairer",
        "prezygapophysial", "cylindric",
        "pneumonalgia", "sleigher",
        "emir", "Socraticism",
        "licitness", "massedly",
        "instructiveness", "sturdied",
        "redecrease", "starosta",
        "evictor", "orgiastic",
        "squdge", "meloplasty",
        "Tsonecan", "repealableness",
        "swoony", "myesthesia",
        "molecule", "autobiographist",
        "reciprocation", "refective",
        "unobservantness", "tricae",
        "ungouged", "floatability",
        "Mesua", "fetlocked",
        "chordacentrum", "sedentariness",
        "various", "laubanite",
        "nectopod", "zenick",
        "sequentially", "analgic",
        "biodynamics", "posttraumatic",
        "nummi", "pyroacetic",
        "bot", "redescend",
        "dispermy", "undiffusive",
        "circular", "trillion",
        "Uraniidae", "ploration",
        "discipular", "potentness",
        "sud", "Hu",
        "Eryon", "plugger",
        "subdrainage", "jharal",
        "abscission", "supermarket",
        "countergabion", "glacierist",
        "lithotresis", "minniebush",
        "zanyism", "eucalypteol",
        "sterilely", "unrealize",
        "unpatched", "hypochondriacism",
        "critically", "cheesecutter"
    )
}
