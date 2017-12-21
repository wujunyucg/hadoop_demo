package demo4;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class InfoBean implements WritableComparable<InfoBean> {
    private String account;
    private double income;
    private double outcome;
    private double surplus;

    public InfoBean() {
        super();
    }

    public InfoBean(String account, double income, double outcome) {
        this.account = account;
        this.income = income;
        this.outcome = outcome;
        this.surplus = income - outcome;
    }

    public int compareTo(InfoBean o) {
        if(this.income == o.getIncome()) {
            return this.outcome > o.getOutcome()? 1: -1;
        } else {
            return this.getIncome() > o.getIncome()? 1: -1;
        }
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(account);
        dataOutput.writeDouble(income);
        dataOutput.writeDouble(outcome);
        dataOutput.writeDouble(surplus);
    }

    public void readFields(DataInput dataInput) throws IOException {
        account = dataInput.readUTF();
        income = dataInput.readDouble();
        outcome = dataInput.readDouble();
        surplus = dataInput.readDouble();
    }

    @Override
    public String toString() {
        return  this.income+"\t"+this.outcome+"\t"+this.surplus;
    }

    public String getAccount() {
        return account;
    }

    public void setAccount(String account) {
        this.account = account;
    }

    public double getIncome() {
        return income;
    }

    public void setIncome(double income) {
        this.income = income;
    }

    public double getOutcome() {
        return outcome;
    }

    public void setOutcome(double outcome) {
        this.outcome = outcome;
    }

    public double getSurplus() {
        return surplus;
    }

    public void setSurplus(double surplus) {
        this.surplus = surplus;
    }
}
