import { sink } from './index'

sink.registerHandler({
  init() {

  },
  collection(cole) {
    var col: sink.CollectionHandler = {
      start(col, data) {
        console.log(col)
      },
      data(data) {
        console.log(data)
      },
      end() {
        console.log('ended ', cole.name)
      }
    }
    return col
  },
  end() {
    console.log('totally ended')
  },
  error(er) {
    console.log('NOOOO')
  }
})
