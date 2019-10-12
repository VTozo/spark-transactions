import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Map;

class Exercicio2 {

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

        // Mapeamento pela coluna de mercadorias
        linhas = linhas.map(l -> l.split(";")[1]);

        // Contagem por valor
        Map<String, Long> contagem = linhas.countByValue();

        // Imprime os valores
        contagem.forEach((x, y) -> System.out.println(y + "\t" + x));

    }

}
