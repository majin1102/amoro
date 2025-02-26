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
import { onMounted, reactive, ref } from 'vue'
import { Modal as AModal, message } from 'ant-design-vue'
import { useI18n } from 'vue-i18n'
import { usePlaceholder } from '@/hooks/usePlaceholder'
import type { IIOptimizeGroupItem } from '@/types/common.type'
import {
  addResourceGroupsAPI,
  getGroupContainerListAPI,
  updateResourceGroupsAPI,
} from '@/services/optimize.service'
import Properties from '@/views/catalogs/Properties.vue'

const props = defineProps<{
  edit: boolean
  editRecord: IIOptimizeGroupItem | null
}>()

const emit = defineEmits<{
  (e: 'cancel'): void
  (e: 'refresh'): void
}>()

const { t } = useI18n()

interface FormState {
  name: string
  container: undefined | string
  properties: { [prop: string]: string }
}

const placeholder = reactive(usePlaceholder())
const selectList = ref<{ containerList: any }>({ containerList: [] })
async function getContainerList() {
  const result = await getGroupContainerListAPI()
  const list = (result || []).map((item: string) => ({
    label: item,
    value: item,
  }))
  selectList.value.containerList = list
}

const formState: FormState = reactive({
  name: '',
  container: undefined,
  properties: {},
})

const confirmLoading = ref<boolean>(false)
function handleCancel() {
  emit('cancel')
}

const formRef = ref()
const propertiesRef = ref()
function handleOk() {
  formRef.value.validateFields().then(async () => {
    try {
      const properties = await propertiesRef.value.getProperties()
      const params = {
        name: formState.name,
        container: formState.container as string,
        properties,
      }
      if (props.edit) {
        await updateResourceGroupsAPI(params)
      }
      else {
        await addResourceGroupsAPI(params)
      }
      message.success(`${t('save')} ${t('success')}`)
      emit('refresh')
    }
    catch (error) {
      message.error(`${t('save')} ${t('failed')}`)
    }
  })
}

onMounted(() => {
  getContainerList()
  if (props.edit) {
    formState.name = props.editRecord?.name as string
    formState.container = props.editRecord?.container
    formState.properties = props.editRecord?.resourceGroup.properties as {
      [props: string]: string
    }
  }
})
</script>

<template>
  <AModal
    :open="true"
    :title="edit ? $t('editGroup') : $t('addGroup')"
    :confirm-loading="confirmLoading"
    :closable="false"
    class="group-modal"
    @ok="handleOk"
    @cancel="handleCancel"
  >
    <a-form ref="formRef" :model="formState" class="label-120">
      <a-form-item name="name" :label="$t('name')" :rules="[{ required: true, message: `${placeholder.groupNamePh}` }]">
        <a-input v-model:value="formState.name" :placeholder="placeholder.groupNamePh" :disabled="edit" />
      </a-form-item>
      <a-form-item
        name="container" :label="$t('container')"
        :rules="[{ required: true, message: `${placeholder.groupContainer}` }]"
      >
        <a-select
          v-model:value="formState.container" :show-search="true" :options="selectList.containerList"
          :placeholder="placeholder.groupContainer"
        />
      </a-form-item>
      <a-form-item :label="$t('properties')" />
      <a-form-item>
        <Properties ref="propertiesRef" :properties-obj="formState.properties" :is-edit="true" />
      </a-form-item>
    </a-form>
  </AModal>
</template>

<style lang="less">
.group-modal {
  .ant-modal-body {
    max-height: 600px;
    overflow: auto;
  }
}
</style>
