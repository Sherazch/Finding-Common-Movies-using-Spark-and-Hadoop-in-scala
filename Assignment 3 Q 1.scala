
%%init_spark
launcher.master="yarn"
launcher.num_executors=6
launcher.executor_memory="2500m"
launcher.executor_cores="2"

def pairwise(mid:Any,uids:Any):String={
    var i=0;
    var j=0;
    val tmp:String=uids+"";
    val splits=tmp.split(",").map(_.trim);
    var temp:String="";
    for(i <- 0 to splits.length-2;j <- 1 to splits.length-1){
           // if(splits(i).toInt < splits(j).toInt)
            temp=temp.concat(splits(i)+","+splits(j)+"#"+mid+"\n");
       // else
        //temp=temp.concat(splits(j)+","+splits(i)+"#"+mid+"\n");     
    }
    return temp.dropRight(1);
}

val ratings=sc.textFile("hdfs://C570BD-HM-Master:9000/hadoop-user/ratings.dat").filter(line=>line != "");
val movies=sc.textFile("hdfs://C570BD-HM-Master:9000/hadoop-user/movies").filter(line=>line != "").
map(line=>(line.split("#")(0),line.split("#")(1)));
val rdd=ratings.map(line=>(line.split("\\s+")(0),line.split("\\s+")(1))).flatMapValues(line=>line.split("#")).
map{case (x,y)=>(y,x)};
val joined=rdd.join(movies);
val rdd1=joined.map{case (x,(y,z))=> (y,z)}.map{case (x,y)=>(y,x)}.groupByKey();
val rdd2=rdd1.map{ case (a,b)=> pairwise(a,b)};
val rdd3=rdd2.flatMap(line=>line.split("\n")).filter(line=>line != "").
map(tmp=>(tmp.split("#")(0),tmp.split("#")(1)));
val result=rdd3.map{case (x,y)=>(x,y)}.groupByKey().map{case (x,y)=>x+" -> "+y.size+" -> "+y};

result.collect().foreach(println);


