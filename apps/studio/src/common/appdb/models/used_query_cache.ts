import { Entity, Column, Index } from 'typeorm'
import { ApplicationEntity } from './application_entity'

@Entity({ name: 'used_query_cache' })
export class UsedQueryCache extends ApplicationEntity {
  withProps(props?: any): UsedQueryCache {
    if (props) UsedQueryCache.merge(this, props);
    return this;
  }

  @Index()
  @Column({ type: "integer", nullable: false })
  usedQueryId!: number

  @Column({ type: "text", nullable: false })
  cachedResults!: string
}
