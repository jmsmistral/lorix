import lorix from './lorix.js';

const sales = lorix.DataFrame.fromArray([
  { team: 'North', units: 2, price: 10 },
  { team: 'South', units: 1, price: 15 },
  { team: 'North', units: 3, price: 10 },
]);

sales
  .withColumn('revenue', (row) => row.units * row.price)
  .groupBy(['team'], { revenue: ['sum', 'mean'] })
  .orderBy(['revenue_sum'], ['desc'])
  .head();
