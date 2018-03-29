# This example is a gremlin tutorial and shows how to query date using gremlin and make recommendations collaborative filtering with a gaming dataset

## Prerequisite

This tutorial assumes you already have your environment setup. To setup a new environment, create an Amazon Neptune Cluster and you can run this cloudformation template [link TBD] to setup 
the additional resources.

See the following links for how to create an Amazon Neptune Cluster for Gremlin:  

* https://docs.aws.amazon.com/neptune/latest/userguide/get-started-CreateInstance-Console.html
* https://docs.aws.amazon.com/neptune/latest/userguide/bulk-load.html
* https://docs.aws.amazon.com/neptune/latest/userguide/bulk-load-tutorial-format-gremlin.html


## Use Case

In this tutorial, we'll traverse console game preferences among a small set of gamers. We'll explore commonality, preferences and potential game recommendations. These queries are for the purposes of learning gremlin and Amazon Neptune. 


![cloudformation](images/image1.jpg)


## Step 1 (Load Data Sample data)


**Game & Player Vertices** (~id,~label,GamerAlias:String,ReleaseDate:Date,GameGenre:String,ESRBRating:String,Developer:String,Platform:String,GameTitle:String)

```
curl -X POST \
    -H 'Content-Type: application/json' \
    http://your-neptune-endpoint:8182/loader -d '
    { 
      "source" : "s3://neptune-data-ml/recommendation/vertex.txt", 
      "accessKey" : "", 
      "secretKey" : "",
      "format" : "csv", 
      "region" : "us-east-1", 
      "failOnError" : "FALSE"
    }'
```

**Edges** (~id, ~from, ~to, ~label, weight:Double) 
```
curl -X POST \
    -H 'Content-Type: application/json' \
    http://your-neptune-endpoint:8182/loader -d '
    { 
      "source" : "s3://neptune-data-ml/recommendation/edges.txt", 
      "accessKey" : "", 
      "secretKey" : "",
      "format" : "csv", 
      "region" : "us-east-1", 
      "failOnError" : "FALSE"
    }'
```

**Tip** Alternatively, you could load all of the files by loading the entire directory
```
curl -X POST \
    -H 'Content-Type: application/json' \
    http://your-neptune-endpoint:8182/loader -d '
    { 
      "source" : "s3://neptune-data-ml/recommendation/", 
      "accessKey" : "", 
      "secretKey" : "",
      "format" : "csv", 
      "region" : "us-east-1", 
      "failOnError" : "FALSE"
    }'
```

**Tip**
Upon executing each above curl command. Amazon Neptune will return a loadId associated with each request. You can query
```
curl http://your-neptune-endpoint:8182/loader?loadId=[loadId value]
```

For more information about loading data into Amazon Neptune visit: https://docs.aws.amazon.com/neptune/latest/userguide/bulk-load.html

## Sample Queries


**Query for a particular vertex (gamer)**

```
gremlin> g.V().hasId('Luke').valueMap()
==>{GamerAlias=[skywalker123]}

gremlin> g.V().has("GamerAlias","skywalker123").valueMap()
==>{GamerAlias=[skywalker123]}

gremlin> g.V().has('GamerAlias','skywalker123')
==>v[Luke]
```

**Sample some of the edges (limit 5)**
```
gremlin> g.E().limit(5)
==>e[e25][Luke-likes->SuperMarioOdyssey]
==>e[e26][Mike-likes->SuperMarioOdyssey]
==>e[e8][Mike-likes->CallOfDutyBO4]
==>e[e1][Luke-likes->HorizonZeroDawn]
==>e[e9][Mike-likes->GranTurismoSport]
```

**Sample some of the vertices (limit 4)**
```
gremlin> g.V().limit(4)
==>v[SuperMarioOdyssey]
==>v[Luke]
==>v[Emma]
==>v[MarioKart8]
```

**Count the in-degree centrality of incoming edges to each vertex**
```
gremlin> g.V().group().by().by(inE().count())
==>{v[HorizonZeroDawn]=2, v[Luke]=0, v[ARMS]=2, v[Ratchet&Clank]=3, v[SuperMarioOdyssey]=3, v[GravityRush]=2, v[CallOfDutyBO4]=1, v[MarioKart8]=3, v[Fifa18]=1, v[Nioh]=1, v[Mike]=0, v[Knack]=2, v[Lina]=0, v[TombRaider]=2, v[GranTurismoSport]=2, v[Emma]=0}
```

**Count the out-degree centrality of outgoing edges from each vertex**
```
gremlin> g.V().group().by().by(outE().count())
==>{v[HorizonZeroDawn]=0, v[Luke]=8, v[ARMS]=0, v[Ratchet&Clank]=0, v[SuperMarioOdyssey]=0, v[GravityRush]=0, v[CallOfDutyBO4]=0, v[MarioKart8]=0, v[Fifa18]=0, v[Nioh]=0, v[Mike]=8, v[Knack]=0, v[Lina]=6, v[TombRaider]=0, v[GranTurismoSport]=0, v[Emma]=2}

```
**Count the out-degree centrality of outgoing edges from each vertex by order of degee**
```
gremlin> g.V().project("v","degree").by().by(bothE().count()).order().by(select("degree"), decr)
==>{v=v[Luke], degree=8}
==>{v=v[Mike], degree=8}
==>{v=v[Lina], degree=6}
==>{v=v[SuperMarioOdyssey], degree=3}
==>{v=v[MarioKart8], degree=3}
==>{v=v[Ratchet&Clank], degree=3}
==>{v=v[Emma], degree=2}
==>{v=v[HorizonZeroDawn], degree=2}
==>{v=v[GranTurismoSport], degree=2}
==>{v=v[ARMS], degree=2}
==>{v=v[GravityRush], degree=2}
==>{v=v[TombRaider], degree=2}
==>{v=v[Knack], degree=2}
==>{v=v[Fifa18], degree=1}
==>{v=v[Nioh], degree=1}
==>{v=v[CallOfDutyBO4], degree=1}
```

**Return only the vertices that are games**
```
gremlin> g.V().hasLabel('game')
==>v[Mario+Rabbids]
==>v[ARMS]
==>v[HorizonZeroDawn]
==>v[GranTurismoSport]
==>v[Ratchet&Clank]
==>v[Fifa18]
==>v[GravityRush]
==>v[Nioh]
==>v[TombRaider]
==>v[CallOfDutyBO4]
==>v[Knack]
==>v[SuperMarioOdyssey]
==>v[MarioKart8]
```

**Return only the vertices that are people**
```
gremlin> g.V().hasLabel('person')
==>v[Luke]
==>v[Emma]
==>v[Lina]
==>v[Mike]
```

**Return counts of games grouped by their genre**
```
gremlin> g.V().hasLabel('game').groupCount().by("GameGenre")
==>{Shooter=2, Action=3, Adventure=5, Racing=2, Sports=1}
```
**Return counts of games grouped by developer**
```
gremlin> g.V().hasLabel('game').groupCount().by("Developer")
==>{Activision=1, Nintendo=3, Square Enix=1, Guerrilla Games=1, Sony Interactive Entertainment=2, Insomniac Games=1, Electronic Arts=1, Project Siren=1, Ubisoft=1, Team Ninja=1}
```

**Return counts of games grouped by Platform**
```
gremlin> g.V().hasLabel('game').groupCount().by("Platform")
==>{PS4=9, Switch=4}
```

**What is the average weighted rating of MarioKart8?**
```
gremlin> g.V().hasLabel('game').has('GameTitle','MarioKart8').inE('likes').values('weight').mean()
==>0.6333333333333334
```


**What games does skywalker123 like?**
```
gremlin> g.V().has('GamerAlias','skywalker123').as('gamer').out('likes')
==>v[ARMS]
==>v[HorizonZeroDawn]
==>v[GranTurismoSport]
==>v[Ratchet&Clank]
==>v[Fifa18]
==>v[GravityRush]
==>v[SuperMarioOdyssey]
==>v[MarioKart8]
```

**What games does skywalker123 like using weight (greater than)**
```
gremlin> g.V().has('GamerAlias','skywalker123').outE("likes").has('weight', P.gt(0.7f))
==>e[e17][Luke-likes->Mario+Rabbids]
==>e[e3][Luke-likes->Ratchet&Clank]
```

**What games does skywalker123 like using weight (less than)**
```
gremlin> g.V().has('GamerAlias','skywalker123').outE("likes").has('weight', P.lt(0.5f))
==>e[e1][Luke-likes->HorizonZeroDawn]
==>e[e2][Luke-likes->GranTurismoSport]
==>e[e4][Luke-likes->Fifa18]
==>e[e5][Luke-likes->GravityRush]
==>e[e21][Luke-likes->MarioKart8]
```

**Who else likes the same games?**
```
gremlin> g.V().has('GamerAlias','skywalker123').out('likes').in('likes').dedup().values('GamerAlias')
==>forchinet
==>skywalker123
==>bringit32
==>smiles007
```

**Who else likes these games (exclude yourself)**
```
gremlin> g.V().has('GamerAlias','skywalker123').as('TargetGamer').out('likes').in('likes').where(neq('TargetGamer')).dedup().values('GamerAlias')
==>forchinet
==>bringit32
==>smiles007
```

**What are other games titles likes by gamers who have commonality?**
```
gremlin> g.V().has('GamerAlias','skywalker123').as('TargetGamer').out('likes').in('likes').where(neq('TargetGamer')).out('likes').dedup().values('GameTitle')
==>ARMs
==>HorizonZeroDawn
==>GranTurismoSport
==>Nioh
==>TombRaider
==>CallOfDutyBO4
==>SuperMarioOdyssey
==>MarioKart8
==>Ratchet&Clank
==>GravityRush
==>Knack
```

**Which games might make sense to recommend to a specific gamer that they don't already like?**
```
gremlin> g.V().has('GamerAlias','skywalker123').as('TargetGamer').out('likes').aggregate('self').in('likes').where(neq('TargetGamer')).out('likes').where(without('self')).dedup().values('GameTitle')
==>Nioh
==>TombRaider
==>CallOfDutyBO4
==>Knack
```

For more recommendation example queries or other gremlin reciepes see: http://tinkerpop.apache.org/docs/current/recipes/#recommendation


**Drop data**
```
gremlin> g.V().drop().iterate()

```

## License

Copyright 2011-2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at

[http://aws.amazon.com/apache2.0/](http://aws.amazon.com/apache2.0/)

or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

