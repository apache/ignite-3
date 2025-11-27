import React from 'react';
import MDXComponents from '@theme-original/MDXComponents';
import { Icon } from '@iconify/react';

const IIcon = ({ icon = 'mdi:information-outline', ...props }) => {
  return <Icon icon={icon} {...props} />;
};

export default {
  ...MDXComponents,
  IIcon,
};
