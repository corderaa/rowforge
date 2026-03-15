package com.rowforge.model;

import java.io.Serializable;
import java.time.Instant;
import java.util.UUID;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.PrePersist;

/**
 * Class that stores information about each dataset generation in the APP
 */
@Entity
public class DatasetGeneration implements Serializable {

    public DatasetGeneration() {

    }

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    private String anom_id;

    private UUID user_id;

    private Integer rows_generated;

    private Integer tables_generated;

    private String db_type;

    private Instant created_at;

    @PrePersist
    public void prePersist() {
        db_type = "NOT DEFINED";
        created_at = Instant.now();
    }

    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public String getAnom_id() {
        return anom_id;
    }

    public void setAnom_id(String anom_id) {
        this.anom_id = anom_id;
    }

    public UUID getUser_id() {
        return user_id;
    }

    public void setUser_id(UUID user_id) {
        this.user_id = user_id;
    }

    public Integer getRows_generated() {
        return rows_generated;
    }

    public void setRows_generated(Integer rows_generated) {
        this.rows_generated = rows_generated;
    }

    public Integer getTables_generated() {
        return tables_generated;
    }

    public void setTables_generated(Integer tables_generated) {
        this.tables_generated = tables_generated;
    }

    public String getDb_type() {
        return db_type;
    }

    public void setDb_type(String db_type) {
        this.db_type = db_type;
    }

    public Instant getCreated_at() {
        return created_at;
    }

    public void setCreated_at(Instant created_at) {
        this.created_at = created_at;
    }

    @Override
    public int hashCode() {
        // TODO Auto-generated method stub
        return super.hashCode();
    }

    @Override
    public String toString() {
        // TODO Auto-generated method stub
        return super.toString();
    }
    
}
