import { emit, log, util } from './index'

util.source(() => {
  emit.collection('yeah')
  emit.data({zo: 213})

  emit.collection('zobi')
  emit.data({beh: 'sfdsfn', bwop: new Date()})
  emit.data({beh: 'sdshfkjh'})
})
