export default {
  preset: 'ts-jest',
  coveragePathIgnorePatterns: [
    'coverage',
    '.build',
    '.sst',
    'test',
    'd.ts',
    'stacks/'
  ],
  coverageThreshold: {
    global: {
      branches: 100,
      functions: 100,
      lines: 100,
      statements: 100
    }
  },
  setupFilesAfterEnv: [
    './test/jest-test-setup.ts'
  ],
  testPathIgnorePatterns: [
    '.js',
    'stacks/'
  ]
}
