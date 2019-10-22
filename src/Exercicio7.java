import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

import java.util.Map;

public class Exercicio7 {
    private static String resultado = "";
    private static long maior = 0;

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

        // Filtragem de campo vazio
        linhas = linhas.filter(l -> !l.split(";")[6].equals(""));

        // Mapeamento pela coluna de ano + mercadorias
        JavaRDD<String> linhasAnoCategoria = linhas.map(l -> l.split(";")[1] + "_" + l.split(";")[3]);

        // Contagem de ocorrências de cada mercadoria por ano
        Map<String, Long> contagem = linhasAnoCategoria.countByValue();

        // Criação do PairRDD para armazenar os valores/pesos
        JavaPairRDD<String, Double> somas = linhas.mapToPair(l -> new Tuple2<>(
                        l.split(";")[1] + "_" + l.split(";")[3],
                        Double.parseDouble(l.split(";")[5])/Double.parseDouble(l.split(";")[6])
                )
        );

        // Soma dos valores/pesos
        somas = somas.reduceByKey(Double::sum);

        // Cálculo da média
        JavaPairRDD<String, Double> resultados = somas.mapToPair(s -> new Tuple2<>(
                s._1(),
                s._2() / contagem.get(s._1())
        ));


        // Encontra os maiores valores/pesos e armazena em "resultado"
        JavaPairRDD<String, Double> resultado = resultados.reduceByKey((x,y) -> {
            if (y > x){
                x = y;
            }
            return x;
        });
        // Imprime o resultado
        System.out.println(resultado.collectAsMap());

    }

}
