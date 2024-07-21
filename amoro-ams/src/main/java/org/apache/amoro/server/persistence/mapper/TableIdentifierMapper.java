/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.amoro.server.persistence.mapper;

import org.apache.amoro.ServerTableIdentifier;
import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Options;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Result;
import org.apache.ibatis.annotations.Results;
import org.apache.ibatis.annotations.Select;

import java.util.List;

public interface TableIdentifierMapper {

  @Insert(
      "INSERT INTO table_identifier(catalog_name, db_name, table_name, format) VALUES("
          + " #{tableIdentifier.catalog}, #{tableIdentifier.database}, #{tableIdentifier.tableName}, "
          + " #{tableIdentifier.format})")
  @Options(useGeneratedKeys = true, keyProperty = "tableIdentifier.id")
  void insertTable(@Param("tableIdentifier") ServerTableIdentifier tableIdentifier);

  @Delete("DELETE FROM table_identifier WHERE table_id = #{tableId}")
  Integer deleteTableIdById(@Param("tableId") long tableId);

  @Delete(
      "DELETE FROM table_identifier WHERE catalog_name = #{catalogName} AND db_name = #{databaseName}"
          + " AND table_name = #{tableName}")
  Integer deleteTableIdByName(
      @Param("catalogName") String catalogName,
      @Param("databaseName") String databaseName,
      @Param("tableName") String tableName);

  @Select(
      "SELECT table_id, catalog_name, db_name, table_name, format FROM table_identifier"
          + " WHERE catalog_name = #{catalogName} AND db_name = #{databaseName} AND table_name = #{tableName}")
  @Results({
    @Result(property = "id", column = "table_id"),
    @Result(property = "tableName", column = "table_name"),
    @Result(property = "database", column = "db_name"),
    @Result(property = "catalog", column = "catalog_name"),
    @Result(property = "format", column = "format"),
  })
  ServerTableIdentifier selectTableIdentifier(
      @Param("catalogName") String catalogName,
      @Param("databaseName") String databaseName,
      @Param("tableName") String tableName);

  @Select(
      "SELECT table_id, catalog_name, db_name, table_name, format FROM table_identifier"
          + " WHERE catalog_name = #{catalogName} AND db_name = #{databaseName}")
  @Results({
    @Result(property = "id", column = "table_id"),
    @Result(property = "catalog", column = "catalog_name"),
    @Result(property = "database", column = "db_name"),
    @Result(property = "tableName", column = "table_name"),
    @Result(property = "format", column = "format")
  })
  List<ServerTableIdentifier> selectTableIdentifiersByDb(
      @Param("catalogName") String catalogName, @Param("databaseName") String databaseName);

  @Select(
      "SELECT table_id, catalog_name, db_name, table_name, format FROM table_identifier"
          + " WHERE catalog_name = #{catalogName}")
  @Results({
    @Result(property = "id", column = "table_id"),
    @Result(property = "catalog", column = "catalog_name"),
    @Result(property = "database", column = "db_name"),
    @Result(property = "tableName", column = "table_name"),
    @Result(property = "format", column = "format")
  })
  List<ServerTableIdentifier> selectTableIdentifiersByCatalog(
      @Param("catalogName") String catalogName);

  @Select("SELECT table_id, catalog_name, db_name, table_name, format FROM table_identifier")
  @Results({
    @Result(property = "id", column = "table_id"),
    @Result(property = "catalog", column = "catalog_name"),
    @Result(property = "database", column = "db_name"),
    @Result(property = "tableName", column = "table_name"),
    @Result(property = "format", column = "format")
  })
  List<ServerTableIdentifier> selectAllTableIdentifiers();
}
