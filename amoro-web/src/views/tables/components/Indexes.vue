<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
/ -->

<script lang="ts" setup>
import { onMounted, reactive, ref, shallowReactive } from 'vue'
import { useI18n } from 'vue-i18n'
import { useRoute } from 'vue-router'
import { usePagination } from '@/hooks/usePagination'
import type { IColumns, IndexDetail, IndexItem } from '@/types/common.type'
import { getIndexDetail, getIndexes } from '@/services/table.service'

const { t } = useI18n()
const route = useRoute()
const query = route.query

const sourceData = reactive({
  catalog: '',
  db: '',
  table: '',
  ...query,
})

const columns: IColumns[] = shallowReactive([
  { title: t('name'), dataIndex: 'name', ellipsis: true },
  { title: t('field'), dataIndex: 'fields', ellipsis: true },
  { title: t('type'), dataIndex: 'type' },
  { title: t('snapshot'), dataIndex: 'snapshot' },
  { title: t('coverage'), dataIndex: 'coverage' },
])

const dataSource = reactive<IndexItem[]>([])
const allItems = reactive<IndexItem[]>([])
const loading = ref<boolean>(false)
const pagination = reactive(usePagination())
const detailMap = reactive<Record<string, IndexDetail>>({})

function updatePageData() {
  dataSource.length = 0
  const start = (pagination.current - 1) * pagination.pageSize
  const end = start + pagination.pageSize
  const pageItems = allItems.slice(start, end)
  pageItems.forEach((item) => {
    dataSource.push(item)
  })
}

async function fetchIndexes() {
  try {
    loading.value = true
    dataSource.length = 0
    allItems.length = 0
    const { catalog, db, table, token } = sourceData as any
    const result = await getIndexes({
      catalog,
      db,
      table,
      token,
    })
    const list = (result || []) as IndexItem[]
    list.forEach((item: IndexItem) => {
      const fieldsArray = Array.isArray(item.fields)
        ? item.fields
        : (item.fields ? String(item.fields).split(',') : [])
      const displayFields = fieldsArray.join(', ')
      allItems.push({
        ...item,
        fields: displayFields,
      })
    })
    pagination.total = allItems.length
    if ((pagination.current - 1) * pagination.pageSize >= pagination.total) {
      pagination.current = 1
    }
    updatePageData()
  }
  catch (error) {
  }
  finally {
    loading.value = false
  }
}

function change({ current = 1, pageSize = 25 }) {
  if (pageSize !== pagination.pageSize) {
    pagination.current = 1
  }
  else {
    pagination.current = current
  }
  pagination.pageSize = pageSize
  updatePageData()
}

async function onExpand(expanded: boolean, record: IndexItem) {
  if (!expanded) {
    return
  }
  if (detailMap[record.name]) {
    return
  }
  try {
    const { catalog, db, table, token } = sourceData as any
    const result = await getIndexDetail({
      catalog,
      db,
      table,
      indexName: record.name,
      token,
    })
    detailMap[record.name] = result as IndexDetail
  }
  catch (error) {
  }
}

function formatValue(value: any): string {
  if (value === null || value === undefined) {
    return ''
  }
  if (typeof value === 'object') {
    try {
      return JSON.stringify(value, null, 2)
    }
    catch {
      return String(value)
    }
  }
  const str = String(value)
  try {
    const parsed = JSON.parse(str)
    return JSON.stringify(parsed, null, 2)
  }
  catch {
    return str
  }
}

onMounted(() => {
  fetchIndexes()
})
</script>

<template>
  <div class="table-indexes">
    <a-table
      row-key="name"
      :columns="columns"
      :data-source="dataSource"
      :pagination="pagination"
      :loading="loading"
      @change="change"
      @expand="onExpand"
    >
      <template #expandedRowRender="{ record }">
        <template v-if="detailMap[record.name]">
          <a-row v-for="(value, key) in detailMap[record.name]" :key="key" type="flex" :gutter="16">
            <a-col flex="220px" style="text-align: right;">
              {{ key }} :
            </a-col>
            <a-col flex="auto">
              <pre class="json-pre">{{ formatValue(value) }}</pre>
            </a-col>
          </a-row>
        </template>
        <div v-else class="empty-detail">
          {{ $t('loading') }}
        </div>
      </template>
    </a-table>
  </div>
</template>

<style lang="less" scoped>
.table-indexes {
  padding: 18px 0;

  .empty-detail {
    padding: 8px 24px;
  }

  :deep(.ant-table-row-expand-icon) {
    border-radius: 0 !important;
  }
}

.json-pre {
  white-space: pre-wrap;
  word-break: break-word;
  font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
  font-size: 12px;
}
</style>
