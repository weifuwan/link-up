package org.apache.cockpit.connectors.hive3.sink.writer;



import java.io.Serializable;
import java.util.List;
import java.util.Optional;

public interface Transaction extends Serializable {


    /** abort prepare commit operation */
    void abortPrepare();

    /**
     * abort prepare commit operation using transaction id
     *
     * @param transactionId transaction id
     */
    void abortPrepare(String transactionId);


    /**
     * when a checkpoint triggered, file sink should begin a new transaction
     *
     * @param checkpointId checkpoint id
     */
    void beginTransaction(Long checkpointId);
}
