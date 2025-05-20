package com.kaori;

import com.kaori.connections.Accumulator;
import com.kaori.connections.OrderLineItemJoiner;
import com.kaori.connections.OrderCustomerJoiner;
import com.kaori.entities.Customer;
import com.kaori.entities.LineItem;
import com.kaori.entities.Order;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class Main {
    public static final OutputTag<String> TAG_LineItem = new OutputTag<String>("LineItem") {
    };
    public static final OutputTag<String> TAG_Order = new OutputTag<String>("Order") {
    };
    public static final OutputTag<String> TAG_Customer = new OutputTag<String>("Customer") {
    };

    public static void main(String[] args) throws Exception {
        System.out.println("The Flink job is setting up the environment....");

        /*
        TPC-H query 3 (mentioned in the paper):
        SELECT l_orderkey, o_orderdate, o_shippriority,
        SUM(l_extendedprice * (1- l_discount)) AS revenue
        FROM Lineitem, Customer, Orders
        WHERE c_mktsegment = 'AUTOMOBILE'
        AND c_custkey = o_custkey AND l_orderkey = o_orderkey
        AND o_orderdate < date '1995-03-13'
        AND l_shipdate > date '1995-03-13'
        GROUP BY l_orderkey, o_orderdate, o_shippriority
         */



        // Set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // This parameter determines the number of final output files.
        env.setParallelism(1);
        String inputPath = "";
        String outputPath = "";
        // Read CSV file as DataStream
        DataStream<String> inputStream = env.readTextFile(inputPath);

        // Process the input and route to side outputs
        SingleOutputStreamOperator<String> routingStream = inputStream.process(new ProcessFunction<String, String>() {

            @Override
            public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                // Extract the table name from the input value
                String tableName = value.substring(1, 3);
                String processedValue = value.charAt(0) + value.substring(3);

                // Route the data to the appropriate side v2 based on the table name
                switch (tableName) {
                    case "LI":
                        ctx.output(TAG_LineItem, processedValue); // LineItem side v2
                        break;

                    case "OR":
                        ctx.output(TAG_Order, processedValue); // Order side v2
                        break;

                    case "CU":
                        ctx.output(TAG_Customer, processedValue); // Customer side v2
                        break;

                    default:
                        out.collect(processedValue); // Handle unexpected cases
                        break;
                }
            }
        });



        /*
         * Extracts three side‐output streams (LineItem, Order, Customer) from routingStream.
         * Each FlatMap splits on “|”, removes an insertion mark, parses fields, filters
         * by business rules (ship date >1995-03-13, order date <1995-03-13, segment=“AUTOMOBILE”),
         * and emits typed objects via collector.collect()
         */
        DataStream<LineItem> lineItemStream = routingStream
                .getSideOutput(TAG_LineItem)
                .flatMap(new FlatMapFunction<String, LineItem>() {
                    @Override
                    public void flatMap(String value, Collector<LineItem> out) throws Exception {
                        String[] fields = value.split("\\|");
                        char insertionMark = fields[0].charAt(0);
                        fields[0] = fields[0].substring(1);

                        LocalDate shipDate = LocalDate.parse(fields[10], DateTimeFormatter.ISO_LOCAL_DATE);
                        if (shipDate.isAfter(LocalDate.parse("1995-03-13", DateTimeFormatter.ISO_LOCAL_DATE))) {
                            out.collect(new LineItem(
                                    insertionMark == '+',
                                    Integer.parseInt(fields[0]),   // l_orderkey
                                    Double.parseDouble(fields[5]), // l_extendedprice
                                    Double.parseDouble(fields[6]), // l_discount
                                    shipDate,                      // l_shipdate
                                    Integer.parseInt(fields[3])    // l_lineNumber
                            ));
                        }
                        // 不满足条件就不 emit，直接丢弃
                    }
                });

        DataStream<Order> orderStream = routingStream
                .getSideOutput(TAG_Order)
                .flatMap(new FlatMapFunction<String, Order>() {
                    @Override
                    public void flatMap(String value, Collector<Order> out) throws Exception {
                        String[] fields = value.split("\\|");
                        char insertionMark = fields[0].charAt(0);
                        fields[0] = fields[0].substring(1);

                        LocalDate orderDate = LocalDate.parse(fields[4], DateTimeFormatter.ISO_LOCAL_DATE);
                        if (orderDate.isBefore(LocalDate.parse("1995-03-13", DateTimeFormatter.ISO_LOCAL_DATE))) {
                            out.collect(new Order(
                                    insertionMark == '+',
                                    Integer.parseInt(fields[0]),  // o_orderkey
                                    Integer.parseInt(fields[1]),  // o_custkey
                                    orderDate,                    // o_orderdate
                                    Integer.parseInt(fields[7])   // o_shippriority
                            ));
                        }
                    }
                });

        DataStream<Customer> customerStream = routingStream
                .getSideOutput(TAG_Customer)
                .flatMap(new FlatMapFunction<String, Customer>() {
                    @Override
                    public void flatMap(String value, Collector<Customer> out) throws Exception {
                        String[] fields = value.split("\\|");
                        char insertionMark = fields[0].charAt(0);
                        fields[0] = fields[0].substring(1);

                        String segment = fields[6];
                        if ("AUTOMOBILE".equals(segment)) {
                            out.collect(new Customer(
                                    insertionMark == '+',
                                    Integer.parseInt(fields[0]), // c_custkey
                                    segment                      // c_mktsegment
                            ));
                        }
                    }
                });



        // Join the parent table (Order) with the child table (Customer) by CustomerKey.
        // For every insertion or deletion in either stream, emit a corresponding OrderCustomerJoiner record downstream.
        DataStream<OrderCustomerJoiner> Customer_Order_Stream = orderStream
                .keyBy(Order::getCustomerKey)                      // Partition Order stream by CustomerKey
                .connect(customerStream.keyBy(Customer::getCustomerKey))  // Connect to Customer stream, also partitioned by CustomerKey
                .process(new CoProcessFunction<Order, Customer, OrderCustomerJoiner>() {
                    // State to buffer Order events keyed by OrderKey; allows joining with Customer when it arrives later.
                    private transient MapState<Integer, Order> orderTuplesState;
                    // State to hold the current Customer record (or null if none); allows joining all buffered Orders.
                    private transient ValueState<Customer> customerTupleState;

                    @Override
                    public void open(Configuration parameters) {
                        // Initialize MapState descriptor for buffering Order tuples by their OrderKey.
                        orderTuplesState = getRuntimeContext().getMapState(
                                new MapStateDescriptor<>(
                                        "parent_records_joint_order_cust",
                                        Integer.class,
                                        Order.class
                                )
                        );
                        // Initialize ValueState descriptor for storing the current Customer tuple.
                        customerTupleState = getRuntimeContext().getState(
                                new ValueStateDescriptor<>(
                                        "child_record_joint_order_cust",
                                        Customer.class
                                )
                        );
                    }

                    // Handle each Order event (parent stream)
                    @Override
                    public void processElement1(
                            Order parentTuple,
                            CoProcessFunction<Order, Customer, OrderCustomerJoiner>.Context context,
                            Collector<OrderCustomerJoiner> collector
                    ) throws Exception {
                        // If this Order event is an insertion:
                        if (parentTuple.isInsertion()) {
                            // 1. Buffer the Order in state by its OrderKey.
                            orderTuplesState.put(parentTuple.getOrderKey(), parentTuple);
                            // 2. If there is a Customer currently in state (i.e., child exists), emit an insertion join.
                            if (customerTupleState.value() != null) {
                                collector.collect(new OrderCustomerJoiner(
                                        true,                              // mark as insertion
                                        parentTuple.getOrderKey(),         // OrderKey
                                        parentTuple.getOrderDate(),        // OrderDate
                                        parentTuple.getShipPriority()      // ShipPriority
                                ));
                            }
                        }
                        // If this Order event is a deletion:
                        else {
                            // 1. Remove the Order from the buffered state.
                            orderTuplesState.remove(parentTuple.getOrderKey());
                            // 2. If a Customer exists, emit a deletion join to retract the previously joined record.
                            if (customerTupleState.value() != null) {
                                collector.collect(new OrderCustomerJoiner(
                                        false,                             // mark as deletion
                                        parentTuple.getOrderKey(),
                                        parentTuple.getOrderDate(),
                                        parentTuple.getShipPriority()
                                ));
                            }
                        }
                    }

                    // Handle each Customer event (child stream)
                    @Override
                    public void processElement2(
                            Customer childTuple,
                            CoProcessFunction<Order, Customer, OrderCustomerJoiner>.Context context,
                            Collector<OrderCustomerJoiner> collector
                    ) throws Exception {
                        // If this Customer event is an insertion:
                        if (childTuple.isInsertion()) {
                            // 1. Update the Customer state to the new tuple (mark as alive).
                            customerTupleState.update(childTuple);
                            // 2. For every buffered Order, emit an insertion join to pair with this Customer.
                            for (Order parentTuple : orderTuplesState.values()) {
                                collector.collect(new OrderCustomerJoiner(
                                        true,                              // mark as insertion
                                        parentTuple.getOrderKey(),
                                        parentTuple.getOrderDate(),
                                        parentTuple.getShipPriority()
                                ));
                            }
                        }
                        // If this Customer event is a deletion:
                        else {
                            // 1. Clear the Customer state (mark as dead).
                            customerTupleState.clear();
                            // 2. For every buffered Order, emit a deletion join to retract previous joins.
                            for (Order parentTuple : orderTuplesState.values()) {
                                collector.collect(new OrderCustomerJoiner(
                                        false,                             // mark as deletion
                                        parentTuple.getOrderKey(),
                                        parentTuple.getOrderDate(),
                                        parentTuple.getShipPriority()
                                ));
                            }
                        }
                    }
                });


        // Connect the LineItem stream (parent) with the OrderCustomerJoiner stream (child) on OrderKey.
        // For each insertion or deletion in either stream, emit a corresponding OrderLineItemJoiner record downstream.
        DataStream<OrderLineItemJoiner> Order_LineItem_Stream = lineItemStream
                .keyBy(LineItem::getOrderKey)                                  // Partition LineItem stream by OrderKey
                .connect(Customer_Order_Stream.keyBy(OrderCustomerJoiner::getOrderKey))  // Connect to joined OrderCustomer stream, partitioned by OrderKey
                .process(new CoProcessFunction<LineItem, OrderCustomerJoiner, OrderLineItemJoiner>() {

                    // State to buffer LineItem events keyed by LineNumber; allows joining with OrderCustomer when it arrives later.
                    private transient MapState<Integer, LineItem> lineItemTuplesState;
                    // State to hold the latest OrderCustomerJoiner record (or null if none); allows joining all buffered LineItems.
                    private transient ValueState<OrderCustomerJoiner> orderTupleState;

                    @Override
                    public void open(Configuration parameters) {
                        // Initialize MapState for buffering LineItem tuples by their LineNumber key.
                        lineItemTuplesState = getRuntimeContext().getMapState(
                                new MapStateDescriptor<>(
                                        "parent_records_joint_lineitem_order",
                                        Integer.class,
                                        LineItem.class
                                )
                        );
                        // Initialize ValueState for storing the current OrderCustomerJoiner tuple.
                        orderTupleState = getRuntimeContext().getState(
                                new ValueStateDescriptor<>(
                                        "child_record_joint_lineitem_order",
                                        OrderCustomerJoiner.class
                                )
                        );
                    }

                    // Handle each LineItem event (parent stream)
                    @Override
                    public void processElement1(
                            LineItem parentTuple,
                            CoProcessFunction<LineItem, OrderCustomerJoiner, OrderLineItemJoiner>.Context context,
                            Collector<OrderLineItemJoiner> collector
                    ) throws Exception {
                        // When the LineItem event is an insertion:
                        if (parentTuple.isInsertion()) {
                            // 1. Buffer the LineItem in state by its LineNumber.
                            lineItemTuplesState.put(parentTuple.getLineNumber(), parentTuple);
                            // 2. If there is a matching OrderCustomerJoiner in state, emit an insertion join.
                            OrderCustomerJoiner child = orderTupleState.value();
                            if (child != null) {
                                collector.collect(new OrderLineItemJoiner(
                                        true,                                // mark as insertion
                                        child.getOrderKey(),                 // OrderKey
                                        child.getOrderDate(),                // OrderDate
                                        child.getShipPriority(),             // ShipPriority
                                        parentTuple.getExtendedPrice(),      // ExtendedPrice
                                        parentTuple.getDiscount()            // Discount
                                ));
                            }
                        }
                        // When the LineItem event is a deletion:
                        else {
                            // 1. Remove the LineItem from buffered state.
                            lineItemTuplesState.remove(parentTuple.getLineNumber());
                            // 2. If a matching OrderCustomerJoiner exists, emit a deletion join to retract the joined record.
                            OrderCustomerJoiner child = orderTupleState.value();
                            if (child != null) {
                                collector.collect(new OrderLineItemJoiner(
                                        false,                               // mark as deletion
                                        child.getOrderKey(),
                                        child.getOrderDate(),
                                        child.getShipPriority(),
                                        parentTuple.getExtendedPrice(),
                                        parentTuple.getDiscount()
                                ));
                            }
                        }
                    }

                    // Handle each OrderCustomerJoiner event (child stream)
                    @Override
                    public void processElement2(
                            OrderCustomerJoiner childTuple,
                            CoProcessFunction<LineItem, OrderCustomerJoiner, OrderLineItemJoiner>.Context context,
                            Collector<OrderLineItemJoiner> collector
                    ) throws Exception {
                        // When the OrderCustomerJoiner event is an insertion:
                        if (childTuple.isInsertion()) {
                            // 1. Update the state to the new child tuple (mark as alive).
                            orderTupleState.update(childTuple);
                            // 2. For every buffered LineItem, emit an insertion join with this child.
                            for (LineItem parent : lineItemTuplesState.values()) {
                                collector.collect(new OrderLineItemJoiner(
                                        true,                                // mark as insertion
                                        childTuple.getOrderKey(),
                                        childTuple.getOrderDate(),
                                        childTuple.getShipPriority(),
                                        parent.getExtendedPrice(),
                                        parent.getDiscount()
                                ));
                            }
                        }
                        // When the OrderCustomerJoiner event is a deletion:
                        else {
                            // 1. Clear the child state (mark as dead).
                            orderTupleState.clear();
                            // 2. For every buffered LineItem, emit a deletion join to retract previous joins.
                            for (LineItem parent : lineItemTuplesState.values()) {
                                collector.collect(new OrderLineItemJoiner(
                                        false,                               // mark as deletion
                                        childTuple.getOrderKey(),
                                        childTuple.getOrderDate(),
                                        childTuple.getShipPriority(),
                                        parent.getExtendedPrice(),
                                        parent.getDiscount()
                                ));
                            }
                        }
                    }
                });



        DataStream<Accumulator> Final_Combined_Stream = Order_LineItem_Stream
                .keyBy(OrderLineItemJoiner::getOrderKey)
                .process(new KeyedProcessFunction<Integer, OrderLineItemJoiner, Accumulator>() {
                    private transient ValueState<Double> sumState;
                    private transient ValueState<Long> timerState;
                    private transient ValueState<Integer> shipPriorityState;
                    private transient ValueState<LocalDate> orderDateState;

                    @Override
                    public void open(Configuration parameters) {
                        sumState = getRuntimeContext().getState(
                                new ValueStateDescriptor<>("sumState", Double.class));
                        timerState = getRuntimeContext().getState(
                                new ValueStateDescriptor<>("timerState", Long.class));
                        shipPriorityState = getRuntimeContext().getState(
                                new ValueStateDescriptor<>("shipPrio", Integer.class));
                        orderDateState = getRuntimeContext().getState(
                                new ValueStateDescriptor<>("orderDate", LocalDate.class));
                    }

                    @Override
                    public void processElement(
                            OrderLineItemJoiner in,
                            Context ctx,
                            Collector<Accumulator> out
                    ) throws Exception {

                        Double cur = sumState.value();
                        if (cur == null) cur = 0.0;
                        double delta = in.getExtendedPrice() * (1 - in.getDiscount());
                        sumState.update(in.isInsertion() ? cur + delta : cur - delta);


                        shipPriorityState.update(in.getShipPriority());
                        orderDateState.update(in.getOrderDate());

                        long now = ctx.timerService().currentProcessingTime();
                        Long prev = timerState.value();
                        if (prev != null) {
                            ctx.timerService().deleteProcessingTimeTimer(prev);
                        }
                        long nextTimer = now + 30_000L;
                        ctx.timerService().registerProcessingTimeTimer(nextTimer);
                        timerState.update(nextTimer);
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Accumulator> out) throws Exception {
                        Integer orderKey = ctx.getCurrentKey();
                        Double finalSum = sumState.value();
                        LocalDate od = orderDateState.value();
                        Integer prio = shipPriorityState.value();

                        out.collect(new Accumulator(orderKey, od, prio, finalSum));

                        sumState.clear();
                        shipPriorityState.clear();
                        orderDateState.clear();
                        timerState.clear();
                    }
                });


        try {
            Path opObject = new Path(outputPath);

            FileSystem fs = FileSystem.get(opObject.toUri());

            if (fs.exists(opObject)) {
                fs.delete(opObject, false);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        Final_Combined_Stream.writeAsText(outputPath, FileSystem.WriteMode.OVERWRITE);



        env.execute("cquirrel");

    }
}

