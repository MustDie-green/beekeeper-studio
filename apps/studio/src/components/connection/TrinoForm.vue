<template>
  <div class="trino-form">
    <div class="with-connection-type">
      <div class="form-group col">
        <label for="authenticationType">Authentication Method</label>
        <select
          name="authenticationType"
          v-model="authType"
        >
          <option
            :key="`${t.value}-${t.name}`"
            v-for="t in authTypes"
            :value="t.value"
          >
            {{ t.name }}
          </option>
        </select>
      </div>
      <common-server-inputs
        :config="config"
        :show-password-form="showPasswordForm"
      />
      <div
        class="alert alert-info"
        v-if="!showPasswordForm"
      >
        <i class="material-icons-outlined">info</i>
        <span>Connecting opens the sign-in page in the default browser. The User field is optional and sets the X-Trino-User header.</span>
      </div>
    </div>
  </div>
</template>

<script>

  import CommonServerInputs from './CommonServerInputs.vue'
  import { TrinoAuthType, TrinoAuthTypes } from '@/lib/db/types'

  export default {
    components: { CommonServerInputs },
    props: ['config'],
    data() {
      return {
        authTypes: TrinoAuthTypes,
        authType: this.config.trinoOptions?.authType || TrinoAuthType.Basic
      }
    },
    computed: {
      showPasswordForm() {
        return this.authType !== TrinoAuthType.OAuth2
      }
    },
    watch: {
      authType() {
        if (!this.config.trinoOptions) {
          this.$set(this.config, 'trinoOptions', {})
        }
        this.config.trinoOptions.authType = this.authType
        // Trino only serves the OAuth2 flow over HTTPS
        if (this.authType === TrinoAuthType.OAuth2 && !this.config.ssl) {
          this.config.ssl = true
        }
      }
    }
  }
</script>
