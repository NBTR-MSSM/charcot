
class FeatureFlagService {
  isEnabled(flag) {
    const envVar = process.env[flag]
    return envVar && envVar === 'true'
  }
}

export default new FeatureFlagService()
