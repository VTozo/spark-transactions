import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Map;

public class Exercicio8 {
    public static void main(String[] args) {

        // Mostra apenas erros e prints
        Logger.getLogger("org").setLevel(Level.ERROR);

        // Habilita o uso de n threads
        SparkConf conf = new SparkConf().setAppName("Transações").setMaster("local[*]");

        // Cria o contexto da aplicacao
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Leitura do arquivo
        JavaRDD<String> linhas = sc.textFile("in/transactions.csv");

        // Filtragem do header
        linhas = linhas.filter(l -> !l.startsWith("country_or_area"));

        // Map do fluxo e ano como chave e valores 1.0 para contagem
        JavaPairRDD<String, Double> AnoFluxo = linhas.mapToPair(l -> {
            String[] valores =l.split(";");
            String ano = valores[1].equals("") ? "0" : valores [1];
            String fluxo = valores[4].equals("") ? "0" : valores [4];
            return new Tuple2<>(ano + "_" + fluxo, 1.0);
        });
        
        //Contagem das ocorrencias por reduce
        JavaPairRDD<String, Double> resultado = AnoFluxo.reduceByKey((x,y) -> x + y);

        // Impressão dos resultados
        resultado.foreach(data -> System.out.println(data._1()+ "\t" + data._2()));

    }

}
