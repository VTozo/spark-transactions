import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Map;

class Exercicio3 {

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

        // Filtragem por país
        linhas = linhas.filter(l -> l.split(";")[0].equals("Brazil"));

        // Filtragem por país
        linhas = linhas.filter(l -> l.split(";")[1].equals("2016"));

        // Mapeamento pela coluna de mercadorias
        linhas = linhas.map(l -> l.split(";")[3]);

        // Contagem por valor
        Map<String, Long> contagem = linhas.countByValue();

        // Encontra os maiores valores e armazena em "resultado"
        contagem.forEach(Exercicio3::defineMaiores);

        // Imprime o resultado
        System.out.println(resultado);

    }

    // Encontra os maiores valores e armazena em "resultado"
    private static void defineMaiores(String x, long y) {

        // Se o valor recebido for o maior encontrado, é armazenado
        if (y > maior) {
            maior = y;
            resultado = y + "\t" + x;
        }
        // Se o valor recebido for igual ao maior, é concatenado ao resultado
        else if (y == maior) {
            resultado += "\n" + y + "\t" + x;
        }

    }

}
