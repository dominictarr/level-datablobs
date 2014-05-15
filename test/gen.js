

var l = 100, i = 0

console.log('ID, foo, bar, baz')
while(l--)
  console.log([i++, l, Math.random(), Date.now()].join(', '))
