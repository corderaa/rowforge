package com.rowforge.repository;

import java.util.UUID;

import org.springframework.data.jpa.repository.JpaRepository;

import com.rowforge.model.DatasetGeneration;

public interface DatasetGenerationRepository extends JpaRepository<DatasetGeneration, UUID> {   
}