package com.kaori.entities;

public class Customer {
    private boolean insertion;
    private int custKey;
    private String mktSegment;

    // No-argument constructor (required for frameworks and serialization)
    public Customer() {}

    // Parameterized constructor
    public Customer(boolean insertion, int custKey, String mktSegment) {
        this.insertion = insertion; // Initialize the new member variable
        this.custKey = custKey;
        this.mktSegment = mktSegment;
    }

    // Getters and Setters
    public boolean isInsertion() { // Getter for insertion
        return insertion;
    }


    public int getCustomerKey() {
        return custKey;
    }


    @Override
    public String toString() {
        return "Customer{" +
                "insertion=" + insertion + // Include insertion in the toString
                ", custKey=" + custKey +
                ", mktSegment='" + mktSegment + '\'' +
                '}';
    }
}