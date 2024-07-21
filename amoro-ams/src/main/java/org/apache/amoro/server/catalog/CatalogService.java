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

package org.apache.amoro.server.catalog;

import org.apache.amoro.api.CatalogMeta;
import org.apache.amoro.config.Configurations;
import org.apache.amoro.exception.AlreadyExistsException;
import org.apache.amoro.exception.IllegalMetadataException;
import org.apache.amoro.exception.ObjectNotExistsException;
import org.apache.amoro.server.persistence.PersistentBase;
import org.apache.amoro.server.persistence.mapper.CatalogMetaMapper;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/** The CatalogService interface defines the operations that can be performed on catalogs. */
public class CatalogService extends PersistentBase {

  protected final Configurations serverConfiguration;

  public CatalogService(Configurations serverConfiguration) {
    this.serverConfiguration = serverConfiguration;
  }

  /**
   * Returns a list of CatalogMeta objects.
   *
   * @return a List of CatalogMeta objects representing the catalog metas available.
   */
  public List<CatalogMeta> listCatalogMetas() {
    return getAs(CatalogMetaMapper.class, CatalogMetaMapper::selectCatalogs);
  }

  /**
   * Returns a list of CatalogMeta objects.
   *
   * @return a List of CatalogMeta objects representing the catalog metas available.
   */
  public Map<String, ServerCatalog> getServerCatalogMap() {
    return getAs(CatalogMetaMapper.class, CatalogMetaMapper::selectCatalogs).stream()
        .collect(
            Collectors.toMap(
                CatalogMeta::getCatalogName,
                catalogMeta ->
                    CatalogBuilder.buildServerCatalog(catalogMeta, serverConfiguration)));
  }

  /**
   * Gets the catalog metadata for the given catalog name.
   *
   * @return the catalog meta information
   */
  public CatalogMeta getCatalogMeta(String catalogName) {
    return getAs(
        CatalogMetaMapper.class, catalogMetaMapper -> catalogMetaMapper.selectCatalog(catalogName));
  }

  /**
   * Checks if a catalog exists.
   *
   * @param catalogName the name of the catalog to check for existence
   * @return true if the catalog exists, false otherwise
   */
  public boolean catalogExist(String catalogName) {
    return getAs(
        CatalogMetaMapper.class,
        catalogMetaMapper -> catalogMetaMapper.selectCatalog(catalogName) != null);
  }

  /**
   * Retrieves the ServerCatalog with the given catalog name.
   *
   * @param catalogName the name of the ServerCatalog to retrieve
   * @return the ServerCatalog object matching the catalogName, or null if no catalog exists
   */
  public ServerCatalog getServerCatalog(String catalogName) {
    return Optional.ofNullable(getCatalogMeta(catalogName))
        .map(catalogMeta -> CatalogBuilder.buildServerCatalog(catalogMeta, serverConfiguration))
        .orElseThrow(() -> new ObjectNotExistsException("Catalog : " + catalogName));
  }

  /**
   * Creates a catalog based on the provided catalog meta information. The catalog name is obtained
   * from the catalog meta.
   *
   * @param catalogMeta the catalog meta information used to create the catalog
   */
  public void createCatalog(CatalogMeta catalogMeta) {
    if (catalogExist(catalogMeta.getCatalogName())) {
      throw new AlreadyExistsException("Catalog " + catalogMeta.getCatalogName());
    }
    // Build to make sure the catalog is valid
    ServerCatalog catalog = CatalogBuilder.buildServerCatalog(catalogMeta, serverConfiguration);
    doAs(CatalogMetaMapper.class, mapper -> mapper.insertCatalog(catalog.getMetadata()));
  }

  /**
   * Drops a catalog with the given name.
   *
   * @param catalogName the name of the catalog to be dropped
   */
  public void dropCatalog(String catalogName) {
    ServerCatalog serverCatalog = getServerCatalog(catalogName);
    if (serverCatalog == null) {
      throw new ObjectNotExistsException("Catalog " + catalogName);
    }

    serverCatalog.dispose();
  }

  /**
   * Updates the catalog with the provided catalog meta information.
   *
   * @param catalogMeta The CatalogMeta object representing the updated catalog information.
   */
  public void updateCatalog(CatalogMeta catalogMeta) {
    ServerCatalog catalog = getServerCatalog(catalogMeta.getCatalogName());
    validateCatalogUpdate(catalog.getMetadata(), catalogMeta);
    doAs(CatalogMetaMapper.class, mapper -> mapper.updateCatalog(catalogMeta));
    catalog.updateMetadata(catalogMeta);
  }

  private void validateCatalogUpdate(CatalogMeta oldMeta, CatalogMeta newMeta) {
    if (!oldMeta.getCatalogType().equals(newMeta.getCatalogType())) {
      throw new IllegalMetadataException("Cannot update catalog type");
    }
  }
}
