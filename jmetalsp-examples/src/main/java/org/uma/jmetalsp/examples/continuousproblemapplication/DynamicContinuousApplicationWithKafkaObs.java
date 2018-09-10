package org.uma.jmetalsp.examples.continuousproblemapplication;

import org.uma.jmetal.solution.DoubleSolution;
import org.uma.jmetalsp.*;
import org.uma.jmetalsp.consumer.ChartConsumer;
import org.uma.jmetalsp.consumer.LocalDirectoryOutputConsumer;
import org.uma.jmetalsp.observeddata.AlgorithmObservedData;
import org.uma.jmetalsp.observeddata.ObservedValue;
import org.uma.jmetalsp.problem.fda.FDA2;
import org.uma.jmetalsp.spark.SparkRuntime;
import org.uma.jmetalsp.spark.streamingdatasource.SimpleSparkStreamingCounterDataSource;
import org.uma.jmetalsp.streamingdatasource.SimpleKafkaStreamingCounterDataSource;
import org.uma.jmetalsp.util.restartstrategy.RestartStrategy;
import org.uma.jmetalsp.util.restartstrategy.impl.CreateNRandomSolutions;
import org.uma.jmetalsp.util.restartstrategy.impl.RemoveNSolutionsAccordingToTheHypervolumeContribution;

import java.io.IOException;
import java.util.List;

/**
 * Example of jMetalSP application.
 * Features:
 * - Algorithm: to choose among NSGA-II, SMPSO, MOCell, and WASF-GA
 * - Problem: Any of the FDA familiy
 * - Spark streaming runtime
 *
 * Steps to compile and run the example:
 * 1. Compile the project:
      mvn package
 * 2. Run the program with the name of the output directory where the fronts will be stored:
      spark-submit --class="org.uma.jmetalsp.examples.continuousproblemapplication.DynamicContinuousApplicationWithSpark" \
      jmetalsp-examples/target/jmetalsp-examples-1.1-SNAPSHOT-jar-with-dependencies.jar outputDirectory
 * 3. At the same time, run the program to simulate the streaming data source that generates time:
      java -cp jmetalsp-externalsource/target/jmetalsp-externalsource-1-SNAPSHOT-jar-with-dependencies.jar \
      org.uma.jmetalsp.externalsources.CounterProvider outputDirectory 2000
 * where "outputDirectory" must the same used in Step 3, and the second argument is the frequency of
 * data generation (2000 milliseconds in this example)
 *
 * @author Antonio J. Nebro <antonio@lcc.uma.es>
 */
public class DynamicContinuousApplicationWithKafkaObs {

  public static void main(String[] args) throws IOException, InterruptedException {
    // STEP 1. Create the problem
    DynamicProblem<DoubleSolution, ObservedValue<Integer>> problem =
            new FDA2();

    // STEP 2. Create the algorithm
    DynamicAlgorithm<List<DoubleSolution>, AlgorithmObservedData> algorithm =
            AlgorithmFactory.getAlgorithm("NSGAII", problem) ;

    algorithm.setRestartStrategy(new RestartStrategy<>(
            //new RemoveFirstNSolutions<>(50),
            new RemoveNSolutionsAccordingToTheHypervolumeContribution<>(50),
            //new RemoveNSolutionsAccordingToTheCrowdingDistance<>(50),
            //new RemoveNRandomSolutions(50),
            new CreateNRandomSolutions<DoubleSolution>()));

    // STEP 3. Create the streaming data source (only one in this example) and register the problem
    SimpleKafkaStreamingCounterDataSource streamingDataSource =
            new SimpleKafkaStreamingCounterDataSource();

    streamingDataSource.getObservable().register(problem);

    // STEP 4. Create the data consumers and register into the algorithm
    DataConsumer<AlgorithmObservedData> localDirectoryOutputConsumer =
            new LocalDirectoryOutputConsumer<DoubleSolution>("/home/cbarba/outputDirectory") ;
    DataConsumer<AlgorithmObservedData> chartConsumer =
            new ChartConsumer<DoubleSolution>() ;

    algorithm.getObservable().register(localDirectoryOutputConsumer);
    algorithm.getObservable().register(chartConsumer) ;

    // STEP 5. Create the application and run
    JMetalSPApplication<
            DoubleSolution,
            DynamicProblem<DoubleSolution, ObservedValue<Integer>>,
            DynamicAlgorithm<List<DoubleSolution>, AlgorithmObservedData>> application;

    application = new JMetalSPApplication<>();

    application.setStreamingRuntime(new KafkaRuntime("counter"))
            .setProblem(problem)
            .setAlgorithm(algorithm)
            .addStreamingDataSource(streamingDataSource,problem)
            .addAlgorithmDataConsumer(localDirectoryOutputConsumer)
            .addAlgorithmDataConsumer(chartConsumer)
            .run();
  }
}
